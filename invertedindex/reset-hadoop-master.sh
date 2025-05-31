echo "[MASTER] Checking for running Hadoop services on critical ports..."
sudo lsof -i :9866 -i :9870 -i :8040 -i :8088 -i :9868 -i :19888 -i :10020

echo "[MASTER] Killing processes on those ports..."
sudo lsof -t -i :9866 -i :9870 -i :8040 -i :8088 -i :9868 -i :19888 -i :10020 | xargs -r sudo kill -9

echo "[MASTER] Cleaning Hadoop temp and logs..."
rm -rf /tmp/hadoop-* /tmp/hsperfdata_* /var/log/hadoop/* $HADOOP_HOME/logs/*

echo "[MASTER] Cleaning HDFS NameNode and DataNode directories..."
rm -rf /opt/hdfs/namenode/*
rm -rf /opt/hdfs/datanode/*

echo "[MASTER] Starting NameNode..."
hdfs --daemon start namenode

echo "[MASTER] Starting DataNode..."
hdfs --daemon start datanode

echo "[MASTER] Starting ResourceManager..."
yarn --daemon start resourcemanager

echo "[MASTER] Starting NodeManager..."
yarn --daemon start nodemanager

echo "[MASTER] Starting SecondaryNameNode..."
hdfs --daemon start secondarynamenode

echo "[MASTER] Starting JobHistoryServer..."
mapred --daemon start historyserver

echo "[MASTER] Services running after restart:"
jps
