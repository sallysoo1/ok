# نزّل ملف Iris
wget http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data -O iris.txt

# تأكد إن الملف موجود
ls -l iris.txt

# أنشئ مجلد في HDFS وارفع الملف
hdfs dfs -mkdir -p /user/cloudera/iris_input
hdfs dfs -put -f iris.txt /user/cloudera/iris_input/
hdfs dfs -ls /user/cloudera/iris_input

cat > centroids.txt <<'EOF'
5.0,3.5,1.3,0.3
6.5,3.0,5.5,2.0
5.8,2.7,4.1,1.0
EOF

# ارفع الملف لـ HDFS
hdfs dfs -mkdir -p /user/cloudera/centroids
hdfs dfs -put -f centroids.txt /user/cloudera/centroids/centroids.txt
hdfs dfs -cat /user/cloudera/centroids/centroids.txt

hdfs dfs -ls /user/cloudera/centroids
////////////////////////////////////////////////////////

hdfs dfs -rm -r -f /user/cloudera/centroids

hdfs dfs -mkdir -p /user/cloudera/centroids
hdfs dfs -put -f centroids.txt /user/cloudera/centroids/


hdfs dfs -ls /user/cloudera/centroids



hdfs dfs -cat /user/cloudera/centroids/centroids.txt
****************************************************************
hdfs dfs -mkdir -p /user/cloudera/centroids
hdfs dfs -put -f centroids.txt /user/cloudera/centroids/centroids.txt
hdfs dfs -ls /user/cloudera/centroids


hdfs dfs -cat /user/cloudera/centroids/centroids.txt



