wget http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data -O iris.txt
hdfs dfs -mkdir /user/cloudera/iris_input
hdfs dfs -put iris.txt /user/cloudera/iris_input
