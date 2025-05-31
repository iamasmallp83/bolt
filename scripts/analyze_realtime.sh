#!/bin/bash

JAVA_PID=$(jps | grep -i surefire | awk '{print $1}')
if [ -z "$JAVA_PID" ]; then
    echo "未找到Java进程"
    exit 1
fi

echo "监控进程: $JAVA_PID"

while true; do
    echo "=== $(date) ==="
    
    # 获取线程状态统计
    jstack $JAVA_PID | grep "java.lang.Thread.State" | sort | uniq -c
    
    # 检查RingBuffer相关线程
    echo "RingBuffer相关线程："
    jstack $JAVA_PID | grep -A 5 -B 2 "publishEvent\|RingBuffer" | head -20
    
    # 检查CPU使用率
    echo "进程CPU使用率："
    ps -p $JAVA_PID -o pid,ppid,pcpu,pmem,time,comm
    
    echo "---"
    sleep 5
done
