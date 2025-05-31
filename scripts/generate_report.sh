#!/bin/bash

DUMP_FILE=$1
REPORT_FILE="testmatch_analysis_$(date +%Y%m%d_%H%M%S).txt"

echo "TestMatch 问题分析报告" > $REPORT_FILE
echo "时间: $(date)" >> $REPORT_FILE
echo "Dump文件: $DUMP_FILE" >> $REPORT_FILE
echo "==============================" >> $REPORT_FILE

{
    echo "1. 线程状态分布:"
    grep "java.lang.Thread.State" $DUMP_FILE | sort | uniq -c
    
    echo -e "\n2. BLOCKED线程详情:"
    grep -A 15 -B 3 "BLOCKED" $DUMP_FILE
    
    echo -e "\n3. RingBuffer相关阻塞:"
    grep -A 15 -B 3 "publishEvent\|RingBuffer" $DUMP_FILE
    
    echo -e "\n4. BackpressureManager相关:"
    grep -A 10 -B 3 "BackpressureManager\|canAcceptRequest" $DUMP_FILE
    
    echo -e "\n5. Disruptor线程状态:"
    grep -A 20 -B 3 "AccountDispatcher\|MatchDispatcher\|ResponseEventHandler" $DUMP_FILE
    
} >> $REPORT_FILE

echo "报告已生成: $REPORT_FILE"
