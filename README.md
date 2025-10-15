su cloudera
cd ~
cp /root/dataset.txt /root/centroids.txt ./
hdfs dfs -mkdir -p /user/cloudera/input
hdfs dfs -put dataset.txt /user/cloudera/input/
hdfs dfs -put centroids.txt /user/cloudera/input/
hdfs dfs -ls /user/cloudera/input/
hdfs dfs -cat /user/cloudera/input/dataset.txt | head -3
