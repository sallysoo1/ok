package com.example.kmeans;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, Text, Text> {

    private int numFeatures = 4;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] sum = new double[numFeatures];
        int count = 0;

        for (Text value : values) {
            String pointStr = value.toString();
            String[] parts = pointStr.split(",");
            double[] point = new double[numFeatures];
            for (int i = 0; i < numFeatures; i++) {
                point[i] = Double.parseDouble(parts[i].trim());
            }
            for (int i = 0; i < numFeatures; i++) {
                sum[i] += point[i];
            }
            count++;
        }

        if (count > 0) {
            String centroidStr = "centroid_" + key.get();
            String newCentroidStr = "";
            for (int i = 0; i < numFeatures; i++) {
                double avg = sum[i] / count;
                newCentroidStr += avg;
                if (i < numFeatures - 1) newCentroidStr += ",";
            }
            context.write(new Text(centroidStr), new Text(newCentroidStr));
        }
    }
}
