#!/bin/bash

rm -rf /root/hadoop-2.3.0/etc/hadoop/* ;
cp /root/ephemeral-hdfs/conf/* /root/hadoop-2.3.0/etc/hadoop/. ;
/root/spark-ec2/copy-dir /root/hadoop-2.3.0 ;
cd /root/hadoop-2.3.0 ;
/root/hadoop-2.3.0/bin/hdfs namenode -format ;
/root/hadoop-2.3.0/sbin/start-dfs.sh ;
cd ;
chmod a+x /root/hadoop-2.3.0/setupHdfs* ;

/root/tachyon/bin/tachyon format ;
/root/tachyon/bin/tachyon-start.sh all Mount ;