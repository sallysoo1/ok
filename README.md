import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeansDriver {
    private static final int MAX_ITERATIONS = 10;
    private static final double CONVERGENCE_THRESHOLD = 0.01;
    private static final int K = 3;
    private static final int NUM_FEATURES = 4;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("K", K);
        conf.setInt("NUM_FEATURES", NUM_FEATURES);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: kmeans <in> <out>");
            System.exit(2);
        }

        String input = otherArgs[0];
        String output = otherArgs[1];
        List<double[]> oldCentroids = readInitialCentroids();
        boolean converged = false;
        int iteration = 0;

        while (iteration < MAX_ITERATIONS && !converged) {
            Path outPath = new Path(output + "_iter" + iteration);
            Job job = Job.getInstance(conf, "KMeans Iteration " + iteration);
            job.setJarByClass(KMeansDriver.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansReducer.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, outPath);

            long startTime = System.currentTimeMillis();
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Iteration " + iteration + " took: " + (endTime - startTime) + " ms");

            // اقرأ new centroids من HDFS output
            List<double[]> newCentroids = readCentroidsFromOutput(conf, outPath);
            converged = checkConvergence(oldCentroids, newCentroids);
            oldCentroids = newCentroids;
            input = outPath.toString(); // للـ iteration الجاية (output السابق input الجديد)
            iteration++;
        }

        System.out.println("K-Means converged in " + iteration + " iterations.");
    }

    private static List<double[]> readInitialCentroids() {
        List<double[]> centroids = new ArrayList<>();
        double[][] initials = {
            {5.1, 3.5, 1.4, 0.2}, // cluster 0
            {7.0, 3.2, 4.7, 1.4}, // 1
            {6.3, 3.3, 6.0, 2.5}  // 2
        };
        for (double[] c : initials) {
            centroids.add(c);
        }
        return centroids;
    }

    private static List<double[]> readCentroidsFromOutput(Configuration conf, Path outPath) throws IOException {
        List<double[]> centroids = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        Path partFile = new Path(outPath, "part-r-00000");
        if (!fs.exists(partFile)) {
            System.err.println("Output file not found: " + partFile);
            return centroids;
        }
        FSDataInputStream in = fs.open(partFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("centroid_")) {
                String[] tabParts = line.split("\t");
                if (tabParts.length < 2) continue;
                String[] featParts = tabParts[1].split(",");
                if (featParts.length != NUM_FEATURES) continue;
                double[] centroid = new double[NUM_FEATURES];
                for (int i = 0; i < NUM_FEATURES; i++) {
                    centroid[i] = Double.parseDouble(featParts[i]);
                }
                // أضف حسب cluster id (من outKey)
                String clusterStr = line.substring(line.indexOf("centroid_") + 9, line.indexOf("\t"));
                int clusterId = Integer.parseInt(clusterStr);
                while (centroids.size() <= clusterId) {
                    centroids.add(null);
                }
                centroids.set(clusterId, centroid);
            }
        }
        IOUtils.closeStream(in);
        return centroids;
    }

    private static boolean checkConvergence(List<double[]> oldC, List<double[]> newC) {
        if (oldC.size() != newC.size()) return false;
        double totalDiff = 0;
        for (int i = 0; i < oldC.size(); i++) {
            if (oldC.get(i) == null || newC.get(i) == null) return false;
            for (int j = 0; j < NUM_FEATURES; j++) {
                totalDiff += Math.abs(oldC.get(i)[j] - newC.get(i)[j]);
            }
        }
        return totalDiff < CONVERGENCE_THRESHOLD;
    }
}
