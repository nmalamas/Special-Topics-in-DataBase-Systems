package tuc_stdb_flink_project;

import java.util.ArrayList;
import java.util.List;

public class SkylineAlgorithms {

    public static List<DataPoint> BNL(List<DataPoint> skyline, DataPoint p) {
        List<DataPoint> result = new ArrayList<>(skyline);

        boolean dominated = false;
        List<DataPoint> toRemove = new ArrayList<>();

        for (DataPoint s : result) {
            if (s.dominates(p)) {
                dominated = true;
                break;
            }
            if (p.dominates(s)) {
                toRemove.add(s);
            }
        }

        if (!dominated) {
            result.removeAll(toRemove);
            result.add(p);
        }

        return result; // ALWAYS ArrayList
    }


    public static int MRDimPartition(DataPoint p,
                                     int dim,
                                     int par,
                                     double max_range) {
        int partition_id = 0;
        int Np = 2*par; // Empirical #partitions
        double val = p.getCoords()[dim];

        double partition_range = max_range / Np;
        double partition_start = 0;
        double partition_end = 0;

        for (int i=0; i <= Np - 1; i++) {
            partition_start = i*partition_range;
            partition_end = (i+1)*partition_range;

            if (partition_start <= val && val <partition_end) {
                partition_id = i;
                return partition_id;
            }
        }
        return partition_id;
    }

    public static int MRGridPartition(DataPoint p, int [] max_intervals) {
        int partition_id = 0;

        double x = p.getCoords()[0];
        double y = p.getCoords()[1];

        // Top left partition check
        if ( (0 <= x && x < max_intervals[0]/2) && (max_intervals[1]/2 <= y && y <max_intervals[1]) ) {
            partition_id = 0;
            return partition_id;
        }
        // Bottom-left partition check
        else if ((0 <= x && x < max_intervals[0]/2) && (0 <= y && y <max_intervals[1]/2) ) {
            partition_id = 1;
            return partition_id;
        }
        // Bottom-right partition check
        else if ((max_intervals[0]/2 <= x && x < max_intervals[0]) && (0 <= y && y <max_intervals[1]/2) ) {
            partition_id = 2;
            return partition_id;
        }
        // Top-right partition check
        else {
            partition_id = -1;
            return partition_id;
        }
    }

    public static int MRAnglePartition(DataPoint p, int par) {
        int partition_id = 0;
        int Np = par; // Empirical #partitions
        double x = p.getCoords()[0];
        double y = p.getCoords()[1];

        // Claclulate the hyperspherical coordinates
        double r = Math.sqrt(x*x + y*y);
        double phi = Math.atan2(y, x); // radians, correct quadrant
        if (phi < 0) phi += Math.PI / 2; // if you restrict to first quadrant


        // Divide the first quadrant into the Np sectors
        double maxAngle = Math.PI / 2;   // 90° in radians
        double partition_angle_range = maxAngle / Np;
        double partition_angle_start = 0;
        double partition_angle_end = 0;

        partition_id = (int) (phi / partition_angle_range);
        if (partition_id >= par) partition_id = par - 1;

        return partition_id;
    }
}
