package tuc_stdb_flink_project;

import java.util.concurrent.atomic.AtomicLong;

public class MetricsPrinter {

    private static final AtomicLong tupleCount = new AtomicLong(0);
    private static final AtomicLong totalIngestionTime = new AtomicLong(0);
    private static final AtomicLong totalMapTime = new AtomicLong(0);
    private static final AtomicLong totalReduceTime = new AtomicLong(0);
    private static final AtomicLong firstIngestTime = new AtomicLong(-1);
    private static final AtomicLong lastIngestTime  = new AtomicLong(-1);

    public static void recordIngestion(long nanos) {
        long now = System.nanoTime();

        tupleCount.incrementAndGet();
        totalIngestionTime.addAndGet(nanos);

        firstIngestTime.compareAndSet(-1, now);
        lastIngestTime.set(now);
    }

    public static void recordMap(long nanos) {
        totalMapTime.addAndGet(nanos);
    }

    public static void recordReduce(long nanos) {
        totalReduceTime.addAndGet(nanos);
    }

    public static void printStats() {
        long n = tupleCount.get();
        if (n == 0) return;
        long start = firstIngestTime.get();
        long end   = lastIngestTime.get();

        double seconds =
                start < 0 || end <= start ? 0.0 :
                        (end - start) / 1_000_000_000.0;

        double throughput =
                seconds == 0 ? 0.0 : tupleCount.get() / seconds;


        System.out.println("---- PERFORMANCE METRICS ----");
        System.out.println("Data points: " + n);
        System.out.println("Avg Ingestion time (µs): " + (totalIngestionTime.get() / n) / 1_000.0);
        System.out.println("Avg Map time (µs): " + (totalMapTime.get() / n) / 1_000.0);
        System.out.println("Avg Reduce time (µs): " + (totalReduceTime.get() / n) / 1_000.0);
        System.out.println("Throughput (tuples/sec): " + throughput);
        System.out.println("-----------------------------");
    }
}

