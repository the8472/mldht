#!/bin/bash

cd work
ulimit -n 65535
java  -Xmx2048m -XX:NewSize=20m -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:GCTimeRatio=19 -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30 -cp ".:../libs/*:../DHTIndexer.jar:../libs/hibernate/*"  -Xdebug -Xrunjdwp:transport=dt_socket,address=127.0.0.1:10044,server=y,suspend=y lbms.plugins.mldht.indexer.Main