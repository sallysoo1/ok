package com.example.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private List<double[]> centroids = new ArrayList<double[]>();
    private int numFeatures = 4;  // لـ IRIS، 4 features (dynamic لو عايز: read from first line)

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initial centroids (hardcoded for test, later from cache file)
        centroids.add(new double[]{5.1, 3.5, 1.4, 0.2});  // Cluster 0
        centroids.add(new double[]{7.0, 3.2, 4.7, 1.4});  // Cluster 1
        centroids.add(new double[]{6.3, 3.3, 6.0, 2.5});  // Cluster 2
        System.out.println("Mapper setup: Loaded " + centroids.size() + " centroids");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("Line read: " + line);  // Debug: شوف the input line

        String[] parts = line.split(",");
        if (parts.length < numFeatures + 1) {
            System.out.println("Skipping invalid line: " + line);  // Debug: skip bad lines
            return;
        }

        double[] point = new double[numFeatures];
        try {
            for (int i = 0; i < numFeatures; i++) {
                point[i] = Double.parseDouble(parts[i].trim());
            }
            System.out.println("Parsed point: " + java.util.Arrays.toString(point));  // Debug: parsed features
        } catch (NumberFormatException e) {
            System.out.println("Parse error: " + e.getMessage());  // Debug: parse fail
            return;
        }

        // Find closest centroid
        int closestCluster = 0;
        double minDist = Double.MAX_VALUE;
        for (int i = 0; i < centroids.size(); i++) {
            double dist = euclideanDistance(point, centroids.get(i));
            if (dist < minDist) {
                minDist = dist;
                closestCluster = i;
            }
        }

        // Build pointStr (only features, ignore class)
        String pointStr = "";
        for (int i = 0; i < numFeatures; i++) {
            pointStr += point[i];
            if (i < numFeatures - 1) pointStr += ",";
        }

        System.out.println("Emitting to cluster " + closestCluster + " point: " + pointStr + " dist: " + minDist);  // Debug: emit info
        context.write(new IntWritable(closestCluster), new Text(pointStr));
    }

    private double euclideanDistance(double[] p1, double[] p2) {
        double sum = 0.0;
        for (int i = 0; i < p1.length; i++) {
            double diff = p1[i] - p2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
}
