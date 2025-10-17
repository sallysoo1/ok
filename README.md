# نزّل ملف Iris
wget http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data -O iris.txt

# تأكد إن الملف موجود
ls -l iris.txt

# أنشئ مجلد في HDFS وارفع الملف
hdfs dfs -mkdir -p /user/cloudera/iris_input
hdfs dfs -put -f iris.txt /user/cloudera/iris_input/
hdfs dfs -ls /user/cloudera/iris_input
