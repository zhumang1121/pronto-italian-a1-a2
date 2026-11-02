/* 自动保存：本机 SQLite 为主，浏览器本地写入队列作断线保护。操作按唯一 ID 去重。 */
const ProgressStore=(()=>{
  const CACHE='pronto-progress-v3',PENDING='pronto-pending-v3:';
  let confirmed=Learning.empty(),revision=0,databaseId=null,outbox=[],state=Learning.empty();
  let db=null,busy=false,online=false,started=false,lastSaved=0,listener=()=>{},retryTimer=null;
  let status={kind:'loading',label:'正在读取已保存进度…'};
  const uuid=()=>globalThis.crypto?.randomUUID?.()||('event-'+Date.now().toString(36)+'-'+Math.random().toString(36).slice(2));
  function announce(remote=false){listener({state,status,remote,ready:started})}
  function rebuild(){state=Learning.reduce(outbox,confirmed)}
  function hasRecords(s){return !!(Object.keys(s?.ratings||{}).length||Object.keys(s?.quiz||{}).length||Object.values(s?.drills||{}).some(a=>a.length)||Object.values(s?.listening?.positions||{}).some(p=>p.time>0||p.completed))}
  function readLocal(){try{return JSON.parse(localStorage.getItem(CACHE)||'null')}catch{return null}}
  function readLegacy(){for(const name of ['pronto-italian-v2','pronto-italian-state']){try{const d=JSON.parse(localStorage.getItem(name)||'null');if(d?.ratings)return Learning.normalizeImport(d).state}catch{}}return null}
  function requestIDB(mode,store,action){return new Promise((resolve,reject)=>{if(!db)return resolve(null);const tx=db.transaction(store,mode),req=action(tx.objectStore(store));let result;req.onsuccess=()=>{result=req.result};tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error)})}
  async function openIDB(){try{db=await new Promise((resolve,reject)=>{const r=indexedDB.open('pronto-autosave-v3',1);r.onupgradeneeded=()=>{r.result.createObjectStore('meta');r.result.createObjectStore('outbox',{keyPath:'id'})};r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error)})}catch{db=null}}
  function cache(){
    const record={version:3,confirmed,revision,databaseId,outbox,lastSaved,savedAt:Date.now()};
    try{localStorage.setItem(CACHE,JSON.stringify(record))}catch{}
    requestIDB('readwrite','meta',s=>s.put(record,'progress')).catch(()=>{});
  }
  function keepEvent(event){
    try{localStorage.setItem(PENDING+event.id,JSON.stringify(event))}catch{}
    requestIDB('readwrite','outbox',s=>s.put(event)).catch(()=>{});
  }
  function removeEvent(id){try{localStorage.removeItem(PENDING+id)}catch{}requestIDB('readwrite','outbox',s=>s.delete(id)).catch(()=>{})}
  function makeEvent(type,payload){return{id:uuid(),type,payload,createdAt:Date.now()}}
  function commit(type,payload){
    const event=makeEvent(type,payload);outbox.push(event);keepEvent(event);Learning.apply(state,event);cache();
    status={kind:'saving',label:'正在自动保存…',pending:outbox.length};announce();void sync();return state;
  }
  async function fetchJSON(path,options={}){
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),8000);
    try{const response=await fetch(path,{...options,signal:controller.signal,cache:'no-store'});const body=await response.json();if(!response.ok)throw Error(body.error||'保存服务不可用');return body}finally{clearTimeout(timer)}
  }
  function ingest(batch){
    const pendingIds=new Set(outbox.map(e=>e.id));let remote=false;
    for(const event of batch.events){if(event.seq>revision){Learning.apply(confirmed,event);revision=event.seq;if(!pendingIds.has(event.id))remote=true}}
    const acknowledged=new Set(batch.events.map(e=>e.id));outbox=outbox.filter(e=>{if(acknowledged.has(e.id)){removeEvent(e.id);return false}return true});
    databaseId=batch.databaseId;rebuild();return remote;
  }
  async function sync(){
    if(busy)return;busy=true;let remote=false;
    try{
      let batch=await fetchJSON('/api/events?after='+revision);
      if(databaseId&&databaseId!==batch.databaseId){confirmed=Learning.empty();revision=0;databaseId=null;batch=await fetchJSON('/api/events?after=0')}
      if(!databaseId&&batch.revision===0&&hasRecords(confirmed)){
        const snapshot=Learning.clone(confirmed);confirmed=Learning.empty();revision=0;
        const migration=makeEvent('import',{snapshot});if(outbox.length)migration.createdAt=Math.min(...outbox.map(e=>e.createdAt))-1;outbox.unshift(migration);keepEvent(migration);
      }
      do{remote=ingest(batch)||remote;if(batch.hasMore)batch=await fetchJSON('/api/events?after='+revision);else break}while(true);
      // 可能是先前已确认的操作重发（例如关闭页面时发送），服务器 INSERT OR IGNORE 防止重复评分。
      while(outbox.length){
        const sending=outbox.slice(0,100),ids=new Set(sending.map(e=>e.id));
        batch=await fetchJSON('/api/events',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({after:revision,events:sending})});
        do{remote=ingest(batch)||remote;if(batch.hasMore)batch=await fetchJSON('/api/events?after='+revision);else break}while(true);
        outbox=outbox.filter(e=>{if(ids.has(e.id)){removeEvent(e.id);return false}return true});rebuild();
      }
      online=true;lastSaved=Date.now();status={kind:'saved',label:'已自动保存到本机',at:lastSaved,pending:0};cache();
    }catch(error){
      online=false;status={kind:'offline',label:location.protocol==='file:'?'当前是文件预览：请用“开始学习.command”开启自动保存版':'已暂存浏览器，正在等待本机保存服务恢复',pending:outbox.length,detail:String(error.message||error)};cache();
      clearTimeout(retryTimer);retryTimer=setTimeout(()=>void sync(),5000);
    }finally{busy=false;announce(remote)}
  }
  async function start(callback){
    listener=callback;await openIDB();const a=readLocal(),b=await requestIDB('readonly','meta',s=>s.get('progress')).catch(()=>null);
    const record=[a,b].filter(Boolean).sort((x,y)=>(y.savedAt||0)-(x.savedAt||0))[0];
    if(record?.version===3){confirmed=record.confirmed?.version===3?record.confirmed:Learning.empty();revision=Number(record.revision)||0;databaseId=record.databaseId||null;lastSaved=record.lastSaved||0;outbox=Array.isArray(record.outbox)?record.outbox:[]}
    else{const legacy=readLegacy();if(legacy)confirmed=legacy}
    const pending=await requestIDB('readonly','outbox',s=>s.getAll()).catch(()=>[]);const all=[...outbox,...(pending||[])];
    try{for(let i=0;i<localStorage.length;i++){const name=localStorage.key(i);if(name?.startsWith(PENDING)){const event=JSON.parse(localStorage.getItem(name));if(event?.id)all.push(event)}}}catch{}
    outbox=[...new Map(all.filter(e=>e&&typeof e.id==='string'&&e.payload&&typeof e.type==='string').map(e=>[e.id,e])).values()].sort((x,y)=>x.createdAt-y.createdAt);
    rebuild();started=true;announce();await sync();
    setInterval(()=>{if(!document.hidden)void sync()},30000);
    document.addEventListener('visibilitychange',()=>{if(!document.hidden)void sync()});
    window.addEventListener('online',()=>void sync());
    window.addEventListener('pagehide',()=>{cache();if(outbox.length&&location.protocol!=='file:')navigator.sendBeacon?.('/api/events',new Blob([JSON.stringify({after:revision,events:outbox.slice(0,100)})],{type:'application/json'}))});
    return state;
  }
  function importProgress(data){const normalized=Learning.normalizeImport(data);commit('import',{snapshot:normalized.state});return normalized.summary}
  function exportProgress(){return{format:'pronto-learning-backup',version:3,exportedAt:new Date().toISOString(),snapshot:Learning.clone(state)}}
  return{start,commit,sync,importProgress,exportProgress,get state(){return state},get status(){return status},get online(){return online},get pending(){return outbox.length}};
})();
