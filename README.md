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


hdfs dfs -ls /user/cloudera/centroids
///////////////////////////////////////////////////

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private List<double[]> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        String centStr = context.getConfiguration().get("centroids");
        if (centStr == null) return;
        Scanner sc = new Scanner(centStr);
        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split(",");
            double[] c = new double[parts.length];
            for (int i = 0; i < parts.length; i++) c[i] = Double.parseDouble(parts[i]);
            centroids.add(c);
        }
        sc.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        String[] parts = line.split(",");
        int dim = 4;
        if (parts.length < dim) return;

        double[] point = new double[dim];
        for (int i = 0; i < dim; i++) point[i] = Double.parseDouble(parts[i]);

        int clusterId = 0;
        double minDist = Double.MAX_VALUE;

        for (int i = 0; i < centroids.size(); i++) {
            double dist = 0;
            for (int j = 0; j < dim; j++)
                dist += Math.pow(point[j] - centroids.get(i)[j], 2);
            if (dist < minDist) {
                minDist = dist;
                clusterId = i;
            }
        }

        context.write(new IntWritable(clusterId), new Text(line));
    }
}

