#!/bin/sh

java -Xdebug -Xrunjdwp:transport=dt_socket,address=127.0.0.1:10044,server=y,suspend=n -Xloggc:logs/gc.log -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -Xmx3G -XX:+UseG1GC -XX:MaxGCPauseMillis=15 -XX:GCTimeRatio=19 -XX:+AggressiveOpts -XX:+UnlockExperimentalVMOptions -XX:+TrustFinalNonStaticFields -XX:+ParallelRefProcEnabled -cp ./lib/*:../target/* the8472.mldht.Launcher
