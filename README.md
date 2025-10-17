import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
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
        if (line.startsWith("centroid")) { // سطر centroid، يضيفه للقائمة حسب index
            String[] parts = line.split(",");
            if (parts.length != NUM_FEATURES + 2) return; // "centroidX,feat1,...featN"
            int clusterId = Integer.parseInt(parts[1]); // parts[0]="centroid", parts[1]=id
            if (clusterId >= K) return;
            double[] centroid = new double[NUM_FEATURES];
            for (int i = 0; i < NUM_FEATURES; i++) {
                centroid[i] = Double.parseDouble(parts[i + 2]);
            }
            // ضع في المكان الصح حسب id (عشان الترتيب)
            while (centroids.size() <= clusterId) {
                centroids.add(null);
            }
            centroids.set(clusterId, centroid);
            return;
        }

        // لو data point (IRIS: feat1,feat2,feat3,feat4,class)
        String[] features = line.split(",");
        if (features.length != NUM_FEATURES + 1) return;
        double[] point = new double[NUM_FEATURES];
        for (int i = 0; i < NUM_FEATURES; i++) {
            point[i] = Double.parseDouble(features[i]);
        }
        String actualClass = features[NUM_FEATURES]; // للدقة لاحقاً، بس مش بنستخدمه هنا

        // احسب أقرب centroid (افترض كل centroids موجودة)
        int closest = 0;
        double minDist = distance(point, centroids.get(0));
        for (int i = 1; i < K; i++) {
            double dist = distance(point, centroids.get(i));
            if (dist < minDist) {
                minDist = dist;
                closest = i;
            }
        }

        // emit: cluster_id, point_features,class (عشان نحتاج الـ class للدقة)
        String outKey = Integer.toString(closest);
        String outValue = "";
        for (int i = 0; i < NUM_FEATURES; i++) {
            outValue += point[i] + ",";
        }
        outValue += actualClass; // أضف الـ class للـ value
        context.write(new Text(outKey), new Text(outValue));
    }

    private double distance(double[] p1, double[] p2) {
        double sum = 0;
        for (int i = 0; i < p1.length; i++) {
            sum += Math.pow(p1[i] - p2[i], 2);
        }
        return Math.sqrt(sum);
    }
}
