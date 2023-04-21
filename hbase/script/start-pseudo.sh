#!/bin/sh

#${HBASE_HOME}/bin/hbase-daemon.sh start zookeeper
#${HBASE_HOME}/bin/hbase-daemon.sh start regionserver
#${HBASE_HOME}/bin/hbase-daemon.sh start master

$HBASE_HOME/bin/start-hbase.sh
tail -f $HBASE_HOME/logs/*
#hbase master start