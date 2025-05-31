#!/bin/bash

echo "启动TestMatch监控..."

# 启动测试（后台运行）
mvn test -Dtest=TestMatch &
TEST_PID=$!

# 等待Java进程启动
sleep 5

# 找到Java进程
JAVA_PID=$(jps -l | grep -i surefire | awk '{print $1}')

if [ -z "$JAVA_PID" ]; then
    echo "未找到Java测试进程"
    exit 1
fi

echo "找到Java进程: $JAVA_PID"

# 每10秒获取一次线程dump
for i in {1..10}; do
    echo "获取第 $i 次线程dump..."
    jstack $JAVA_PID > "thread_dump_$i.txt"
    
    # 检查是否还在运行
    if ! kill -0 $JAVA_PID 2>/dev/null; then
        echo "进程已结束"
        break
    fi
    
    sleep 10
done

echo "监控完成，检查 thread_dump_*.txt 文件"
