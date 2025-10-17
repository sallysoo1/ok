import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    private List<double[]> centroids = new ArrayList<>();
    private int K;
    private int NUM_FEATURES;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 3);
        NUM_FEATURES = conf.getInt("NUM_FEATURES", 4);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("centroid")) { // سطر centroid، يضيفه للقائمة
            String[] parts = line.split(",");
            double[] centroid = new double[NUM_FEATURES];
            for (int i = 1; i <= NUM_FEATURES; i++) { // parts[0] = "centroid"
                centroid[i-1] = Double.parseDouble(parts[i]);
            }
            centroids.add(centroid);
            return;
        }

        // لو data point
        String[] features = line.split(",");
        if (features.length != NUM_FEATURES + 1) return; // skip invalid
        double[] point = new double[NUM_FEATURES];
        for (int i = 0; i < NUM_FEATURES; i++) {
            point[i] = Double.parseDouble(features[i]);
        }
        // آخر feature هو class label، مش بنستخدمه في الـ clustering

        // احسب أقرب centroid
        int closest = 0;
        double minDist = distance(point, centroids.get(0));
        for (int i = 1; i < K; i++) {
            double dist = distance(point, centroids.get(i));
            if (dist < minDist) {
                minDist = dist;
                closest = i;
            }
        }

        // emit: cluster_id, point (بدون class label للـ reduce)
        String outKey = Integer.toString(closest);
        String outValue = "";
        for (int i = 0; i < NUM_FEATURES; i++) {
            outValue += point[i] + ",";
        }
        context.write(new Text(outKey), new Text(outValue.substring(0, outValue.length() - 1)));
    }

    private double distance(double[] p1, double[] p2) {
        double sum = 0;
        for (int i = 0; i < p1.length; i++) {
            sum += Math.pow(p1[i] - p2[i], 2);
        }
        return Math.sqrt(sum);
    }
}
