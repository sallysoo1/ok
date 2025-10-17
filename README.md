import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {
    private int NUM_FEATURES;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        NUM_FEATURES = conf.getInt("NUM_FEATURES", 4);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<double[]> points = new ArrayList<>();
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length != NUM_FEATURES + 1) continue; // feats + class
            double[] point = new double[NUM_FEATURES];
            for (int i = 0; i < NUM_FEATURES; i++) {
                point[i] = Double.parseDouble(parts[i]);
            }
            points.add(point); // تجاهل class هنا
        }

        if (points.isEmpty()) return;

        // احسب average
        double[] sum = new double[NUM_FEATURES];
        for (double[] p : points) {
            for (int i = 0; i < NUM_FEATURES; i++) {
                sum[i] += p[i];
            }
        }
        double count = points.size();
        for (int i = 0; i < NUM_FEATURES; i++) {
            sum[i] /= count;
        }

        // emit new centroid (بدون class)
        String outKey = "centroid_" + key.toString();
        String outValue = "";
        for (double f : sum) {
            outValue += f + ",";
        }
        context.write(new Text(outKey), new Text(outValue.substring(0, outValue.length() - 1)));
    }
}
