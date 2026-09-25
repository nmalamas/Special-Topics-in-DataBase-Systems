package tuc_stdb_flink_project;

import java.io.Serializable;
import java.util.Arrays;

public class DataPoint implements Serializable {
    private int id;
    private double[] coords;
    private int partitionId;

    private long ingestTimestamp;
    private long mapTimeNanos;

    public DataPoint() {}

    public DataPoint(int id, double[] coords) {
        this.id = id;
        this.coords = coords;
    }

    public boolean dominates(DataPoint other) {
        boolean strictlyBetter = false;
        for (int d = 0; d < coords.length; d++) {
            if (this.coords[d] > other.coords[d]) {
                return false;
            }
            if (this.coords[d] < other.coords[d]) {
                strictlyBetter = true;
            }
        }
        return strictlyBetter;
    }

    @Override
    public String toString() {
        return String.format("{\"id\": %d, \"coords\": %s}", id, Arrays.toString(coords));
    }

    public void printPointInfo() {
        System.out.print("Pid: "+this.getId()+" (");

        for (int i=0; i < this.getCoords().length; i++) {
            if (i == this.getCoords().length - 1) {
                System.out.println(String.format("%.5f",this.getCoords()[i]) + ")");
                return;
            }
            System.out.print(String.format("%.5f",this.getCoords()[i]) + ", ");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataPoint)) return false;
        DataPoint other = (DataPoint) o;
        return this.id == other.id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }

    // getters / setters
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public double[] getCoords() { return coords; }
    public void setCoords(double[] coords) { this.coords = coords; }
    public long getMapTimeNanos() {
        return mapTimeNanos;
    }
    public void setMapTimeNanos(long mapTimeNanos) {
        this.mapTimeNanos = mapTimeNanos;
    }
    public int getPartitionId() { return partitionId; }
    public void setPartitionId(int partitionId) { this.partitionId = partitionId; }
    public long getIngestTimestamp() { return ingestTimestamp; }
    public void setIngestTimestamp(long t) { ingestTimestamp = t; }
}
