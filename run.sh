#!/bin/bash

echo "===== Start Run: $(TZ='Asia/Shanghai' date) ====="

python main.py > log.txt 2>&1

echo "===== End Run: $(TZ='Asia/Shanghai' date) ====="
