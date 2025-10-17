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
    private int numFeatures = 4;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        centroids.add(new double[]{5.1, 3.5, 1.4, 0.2});
        centroids.add(new double[]{7.0, 3.2, 4.7, 1.4});
        centroids.add(new double[]{6.3, 3.3, 6.0, 2.5});
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        if (parts.length < numFeatures + 1) return;

        double[] point = new double[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            point[i] = Double.parseDouble(parts[i].trim());
        }

        int closestCluster = 0;
        double minDist = Double.MAX_VALUE;
        for (int i = 0; i < centroids.size(); i++) {
            double dist = euclideanDistance(point, centroids.get(i));
            if (dist < minDist) {
                minDist = dist;
                closestCluster = i;
            }
        }

        String pointStr = "";
        for (int i = 0; i < numFeatures; i++) {
            pointStr += point[i];
            if (i < numFeatures - 1) pointStr += ",";
        }
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
