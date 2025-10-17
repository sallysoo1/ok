// Parallel K-Means using MapReduce
// Assumptions: K=3, maxIter=20, threshold=0.001, d=4 (dynamic from data)
// Input: Data file (e.g., iris.data) with lines: f1,f2,...,fd,class (ignore class)
// Output: Final centroids and assignments

// Step 0: Initialize centroids (e.g., first K points from data)
// Write initial centroids to a file: centroids.txt (one line per centroid: id:f1,f2,...,fd)
// e.g., 0:5.1,3.5,1.4,0.2
// 1:7.0,3.2,4.7,1.4
// 2:6.3,3.3,6.0,2.5

boolean converged = false;
int iter = 0;
while (!converged && iter < maxIter) {
    // MapReduce Job: Assign points to clusters
    Map(key: none, value: line) {  // Read line from input file
        Parse line to double[] point = [f1, f2, ..., fd];  // Ignore class
        double minDist = Infinity;
        int closestCluster = -1;
        For each centroid c in broadcasted centroids (from cache file):
            double dist = euclideanDistance(point, c);
            if (dist < minDist) {
                minDist = dist;
                closestCluster = c.id;
            }
        Emit(closestCluster, point);  // Key: int cluster_id, Value: double[] point
    }

    Reduce(key: cluster_id, values: list of points) {
        double[] sum = new double[d];  // d from first point
        int count = 0;
        For each point in values:
            for i=0 to d-1: sum[i] += point[i];
            count++;
        double[] newCentroid = sum / count;
        Emit("centroid:" + cluster_id, newCentroid);  // Output new centroid
    }

    // After job: Read output, parse new centroids
    // Compare to old: if max change < threshold, converged = true;
    iter++;
    // Update centroids.txt for next iteration
}

// Final Job: Assign points to final centroids for evaluation
// Similar to assign job, output: cluster_id, point, true_class for accuracy

// Helper: euclideanDistance(p, c) = sqrt( sum_{i=1 to d} (p_i - c_i)^2 )
