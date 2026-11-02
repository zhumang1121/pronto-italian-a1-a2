#!/usr/bin/env python3
"""本机专用学习站；每次操作以事务写入 SQLite，页面更新不影响数据目录。"""
import argparse
import json
import re
import sqlite3
import mimetypes
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

APP_ID = 'pronto-local-learning-v3'
APP_VERSION = '4.0-listening'
EVENT_TYPES = {'rate','choose-unit','preference','session-start','quiz-answer','quiz-reset','drill-correct','listening','import','reset'}

def create_database(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        db.execute('INSERT OR IGNORE INTO meta VALUES (?,?)',('database_id',str(uuid4())))
        db.execute('CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL, body TEXT NOT NULL)')

def make_handler(root, database, port):
    lock = threading.Lock()
    origins = {f'http://127.0.0.1:{port}',f'http://localhost:{port}'}
    hosts = {f'127.0.0.1:{port}',f'localhost:{port}'}
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self,*args,**kwargs): super().__init__(*args,directory=str(root),**kwargs)
        def log_message(self,fmt,*args): pass
        def permitted(self): return self.headers.get('Host') in hosts
        def send_json(self,status,data):
            raw=json.dumps(data,ensure_ascii=False).encode('utf8')
            self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8')
            self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        def end_headers(self):
            self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Referrer-Policy','no-referrer')
            self.send_header('Cache-Control','no-store')
            super().end_headers()
        def read_events(self,after):
            with sqlite3.connect(database) as db:
                database_id=db.execute('SELECT value FROM meta WHERE key=?',('database_id',)).fetchone()[0]
                revision=db.execute('SELECT COALESCE(MAX(seq),0) FROM events').fetchone()[0]
                rows=db.execute('SELECT seq,body FROM events WHERE seq>? ORDER BY seq LIMIT 1000',(after,)).fetchall()
            return {'databaseId':database_id,'revision':revision,'events':[{'seq':seq,**json.loads(body)} for seq,body in rows], 'nextAfter':rows[-1][0] if rows else after,'hasMore':bool(rows and rows[-1][0]<revision)}
        def send_audio(self,path,head=False):
            target=root/path.lstrip('/')
            if not target.is_file():return self.send_error(404)
            size=target.stat().st_size;start=0;end=size-1
            header=self.headers.get('Range')
            if header:
                match=re.fullmatch(r'bytes=(\d*)-(\d*)',header)
                if not match or not any(match.groups()):return self.send_json(416,{'error':'Invalid audio range'})
                left,right=match.groups()
                if left:
                    start=int(left);end=min(int(right),end) if right else end
                else:start=max(0,size-int(right))
                if start>=size or start>end:
                    self.send_response(416);self.send_header('Content-Range',f'bytes */{size}');self.end_headers();return
            length=end-start+1
            self.send_response(206 if header else 200)
            self.send_header('Content-Type',mimetypes.guess_type(str(target))[0] or 'application/octet-stream')
            self.send_header('Accept-Ranges','bytes');self.send_header('Content-Length',str(length))
            if header:self.send_header('Content-Range',f'bytes {start}-{end}/{size}')
            self.end_headers()
            if head:return
            try:
                with target.open('rb') as f:
                    f.seek(start);remaining=length
                    while remaining:
                        block=f.read(min(65536,remaining))
                        if not block:break
                        self.wfile.write(block);remaining-=len(block)
            except (BrokenPipeError,ConnectionResetError):pass
        def do_HEAD(self):
            if not self.permitted():return self.send_json(403,{'error':'只接受本机学习站地址。'})
            path=urlsplit(self.path).path
            if re.fullmatch(r'/audio/(?:joanna|yukari|trentino|ferragni|martina)\.(?:mp3|wav)',path):return self.send_audio(path,True)
            return super().do_HEAD()
        def do_GET(self):
            if not self.permitted(): return self.send_json(403,{'error':'只接受本机学习站地址。'})
            path=urlsplit(self.path)
            if path.path=='/api/health':return self.send_json(200,{'app':APP_ID,'version':APP_VERSION,'storage':'sqlite','dataDirectory':str(database.parent)})
            if path.path=='/api/events':
                try: after=max(0,int(parse_qs(path.query).get('after',['0'])[0]))
                except ValueError:return self.send_json(400,{'error':'Invalid revision'})
                return self.send_json(200,self.read_events(after))
            if re.fullmatch(r'/audio/(?:joanna|yukari|trentino|ferragni|martina)\.(?:mp3|wav)',path.path):return self.send_audio(path.path)
            allowed={'/','/index.html','/styles.css','/app.js','/vocabulary.js','/roots.js','/listening-data.js','/listening.js','/lessons.js','/curriculum.js','/learning-core.js','/progress-store.js','/favicon.svg'}
            if path.path not in allowed:return self.send_error(404)
            return super().do_GET()
        def do_OPTIONS(self): self.send_json(403,{'error':'不接受其他网站调用。'})
        def do_POST(self):
            if not self.permitted() or self.headers.get('Origin') not in origins|{None}:return self.send_json(403,{'error':'只允许从本机学习页面保存。'})
            if urlsplit(self.path).path!='/api/events':return self.send_error(404)
            if self.headers.get_content_type()!='application/json':return self.send_json(415,{'error':'JSON required'})
            try:
                length=int(self.headers.get('Content-Length','0'))
                if length<=0 or length>2_000_000:raise ValueError('数据大小不合法')
                request=json.loads(self.rfile.read(length)); events=request.get('events',[]);after=max(0,int(request.get('after',0)))
                if not isinstance(events,list) or not 1<=len(events)<=100:raise ValueError('无效的操作列表')
                for event in events:
                    if not isinstance(event,dict) or event.get('type') not in EVENT_TYPES or not isinstance(event.get('payload'),dict):raise ValueError('无效的操作')
                    if not re.fullmatch(r'[A-Za-z0-9_-]{8,120}',str(event.get('id',''))):raise ValueError('无效的操作编号')
                    if not isinstance(event.get('createdAt'),(int,float)):raise ValueError('无效的操作时间')
                with lock,sqlite3.connect(database,timeout=15) as db:
                    db.execute('PRAGMA synchronous=FULL')
                    for event in events:db.execute('INSERT OR IGNORE INTO events(event_id,body) VALUES (?,?)',(event['id'],json.dumps(event,ensure_ascii=False)))
                self.daily_backup()
                return self.send_json(200,self.read_events(after))
            except (ValueError,TypeError,KeyError,json.JSONDecodeError) as exc:return self.send_json(400,{'error':str(exc)})
            except sqlite3.Error:return self.send_json(500,{'error':'本机数据库暂时无法写入。浏览器会保留待保存操作并自动重试。'})
        def daily_backup(self):
            try:
                folder=database.parent/'backups';folder.mkdir(exist_ok=True)
                target=folder/('progress-'+datetime.now().strftime('%Y-%m-%d')+'.sqlite3')
                if not target.exists():
                    with lock,sqlite3.connect(database) as source,sqlite3.connect(target) as dest:source.backup(dest)
            except (OSError,sqlite3.Error):pass # 主库写入成功时，备份失败不伪报保存失败。
    return Handler

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=8768)
    parser.add_argument('--data-dir',type=Path,default=Path.home()/'Documents'/'Codex'/'ProntoLearning'/'data')
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent/'dist')
    parser.add_argument('--open',action='store_true')
    parser.add_argument('--launch',action='store_true',help='在后台启动，打开学习页后退出启动器')
    parser.add_argument('--no-open',action='store_true',help=argparse.SUPPRESS)
    args=parser.parse_args()
    address=f'http://127.0.0.1:{args.port}'
    try:
        with urllib.request.urlopen(address+'/api/health',timeout=1) as response:existing=json.load(response)
        if existing.get('app')==APP_ID:
            if (args.open or args.launch) and not args.no_open:webbrowser.open(address)
            print('学习站已经在运行：'+address);return
        raise RuntimeError('该端口已被其他程序使用，请更换 --port。')
    except (OSError,ValueError):pass
    if args.launch:
        args.data_dir.mkdir(parents=True,exist_ok=True)
        with (args.data_dir/'server.log').open('a',encoding='utf8') as log:
            process=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--port',str(args.port),'--data-dir',str(args.data_dir.resolve()),'--root',str(args.root.resolve())],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
        for _ in range(50):
            try:
                with urllib.request.urlopen(address+'/api/health',timeout=.5) as response:health=json.load(response)
                if health.get('app')==APP_ID:
                    if not args.no_open:webbrowser.open(address)
                    print('学习站已开启。可以关闭启动窗口，进度会自动保存。');return
            except (OSError,ValueError):pass
            if process.poll() is not None:break
            time.sleep(.1)
        raise RuntimeError('学习站没有成功启动，请检查 '+str(args.data_dir/'server.log'))
    database=args.data_dir.resolve()/'progress.sqlite3';create_database(database)
    server=ThreadingHTTPServer(('127.0.0.1',args.port),make_handler(args.root.resolve(),database,args.port))
    print('Pronto! 自动保存版：'+address,flush=True)
    print('进度将自动保存到：'+str(database),flush=True)
    print('下次双击“开始学习.command”继续，进度会自动读取。',flush=True)
    if args.open and not args.no_open:webbrowser.open(address)
    try:server.serve_forever()
    except KeyboardInterrupt:print('\n学习站已关闭，进度已保留。')
    finally:server.server_close()

if __name__=='__main__':main()
