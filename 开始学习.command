#!/bin/zsh
cd "$(dirname "$0")" || exit 1
python3 server.py --launch
if [ $? -ne 0 ]; then
  printf '\n启动未成功。请将这个窗口中的提示发给我。按回车关闭。\n'
  read -r pronto_answer
fi
