#!/bin/bash

java -Xmx2048m -XX:NewSize=20m -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:GCTimeRatio=19 -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30  -cp ".:libs/*:DHTIndexer.jar:libs/hibernate/*" lbms.plugins.mldht.indexer.Main