import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
        List<double[]> oldCentroids = readInitialCentroids(); // اقرأ initial من file أو hardcode
        boolean converged = false;
        int iteration = 0;

        while (iteration < MAX_ITERATIONS && !converged) {
            Path outPath = new Path(output + "_iter" + iteration);
            Job job = Job.getInstance(conf, "KMeans Iteration " + iteration);
            job.setJarByClass(KMeansDriver.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansReducer.class); // optional للـ speed
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

            // اقرأ new centroids من output
            List<double[]> newCentroids = readCentroidsFromOutput(outPath.toString() + "/part-r-00000");
            converged = checkConvergence(oldCentroids, newCentroids);
            oldCentroids = newCentroids;
            input = outPath.toString();
            iteration++;
        }

        System.out.println("K-Means converged in " + iteration + " iterations.");
    }

    private static List<double[]> readInitialCentroids() {
        List<double[]> centroids = new ArrayList<>();
        double[][] initials = {
            {5.1, 3.5, 1.4, 0.2}, // setosa
            {7.0, 3.2, 4.7, 1.4}, // versicolor
            {6.3, 3.3, 6.0, 2.5}  // virginica
        };
        for (double[] c : initials) {
            centroids.add(c);
        }
        return centroids;
    }

    private static List<double[]> readCentroidsFromOutput(String outputFile) throws IOException {
        List<double[]> centroids = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(outputFile));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("centroid_")) {
                String[] parts = line.split("\t")[1].split(",");
                double[] centroid = new double[NUM_FEATURES];
                for (int i = 0; i < NUM_FEATURES; i++) {
                    centroid[i] = Double.parseDouble(parts[i]);
                }
                centroids.add(centroid);
            }
        }
        reader.close();
        return centroids;
    }

    private static boolean checkConvergence(List<double[]> oldC, List<double[]> newC) {
        if (oldC.size() != newC.size()) return false;
        double totalDiff = 0;
        for (int i = 0; i < oldC.size(); i++) {
            for (int j = 0; j < NUM_FEATURES; j++) {
                totalDiff += Math.abs(oldC.get(i)[j] - newC.get(i)[j]);
            }
        }
        return totalDiff < CONVERGENCE_THRESHOLD;
    }
}
