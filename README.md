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
77777777777777777777777777777777777777777777777777777
cat > KMeansMapper.java <<'EOF'
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
EOF
55555555555555555555555555555555555555555
cat > KMeansReducer.java <<'EOF'
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int dim = 4; // عدد الأعمدة (Iris dataset فيها 4 أعمدة رقمية)
        double[] sum = new double[dim];

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            for (int i = 0; i < dim; i++) {
                sum[i] += Double.parseDouble(parts[i]);
            }
            count++;
        }

        if (count == 0) return;

        // حساب المتوسط لكل عمود
        for (int i = 0; i < dim; i++) {
            sum[i] /= count;
        }

        // نجهز المخرجات كـ String
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dim; i++) {
            sb.append(sum[i]);
            if (i < dim - 1) sb.append(",");
        }

        context.write(key, new Text(sb.toString()));
    }
}
EOF

7777777777777777777777777777777777777777777777777
cat > KMeansDriver.java <<'EOF'
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class KMeansDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: KMeansDriver <input> <output> <centroids_hdfs_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // اقرأ ملف الـ centroids من HDFS
        String centPath = args[2];
        FileSystem fs = FileSystem.get(new URI(centPath), conf);
        Path p = new Path(centPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            if (!line.trim().isEmpty()) {
                sb.append(line).append("\n");
            }
        }
        br.close();

        // حطّ المراكز في configuration عشان المابر يعرفها
        conf.set("centroids", sb.toString());

        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansDriver.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
EOF
757575757575555555555555555555555555


hadoop com.sun.tools.javac.Main KMeansMapper.java KMeansReducer.java KMeansDriver.java
