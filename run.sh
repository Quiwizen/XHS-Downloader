#!/bin/bash
# 进入项目目录
cd "$(dirname "$0")"
# 激活虚拟环境
source venv/bin/activate
# 启动程序
python main.py
