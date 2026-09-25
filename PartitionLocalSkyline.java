package tuc_stdb_flink_project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PartitionLocalSkyline implements Serializable {
    private int partitionId;
    private List<DataPoint> skyline;
    private long mapTimeNanos;

    public PartitionLocalSkyline () {}

    public PartitionLocalSkyline(int partitionId,
                                 List<DataPoint> skyline,
                                 long mapTimeNanos) {
        this.partitionId = partitionId;
        this.skyline = new ArrayList<>(skyline);
        this.mapTimeNanos = mapTimeNanos;
    }


    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int pid) {
        this.partitionId = pid;
    }

    public List<DataPoint> getSkyline() {
        return skyline;
    }

    public void setSkyline(List<DataPoint> skyline) {
        this.skyline = new ArrayList<>(skyline);
    }

    public long getMapTimeNanos() {
        return mapTimeNanos;
    }

    public void setMapTimeNanos(long mapTimeNanos) {
        this.mapTimeNanos = mapTimeNanos;
    }
}
