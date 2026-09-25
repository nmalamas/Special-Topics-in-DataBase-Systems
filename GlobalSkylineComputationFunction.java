package tuc_stdb_flink_project;

import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GlobalSkylineComputationFunction extends KeyedCoProcessFunction<Integer, PartitionLocalSkyline, Integer, String> {

    private transient ListState<DataPoint> allLocalSkylinePoints;
    private ListState<DataPoint> globalSkyline;
    private ValueState<Long> totalReduceTime;
    private ValueState<Long> reduceCount;
    private ValueState<Long> totalMapTime;
    private ValueState<Long> mapCount;
    private MapState<Integer, List<DataPoint>> localSkylinesByPartition;


    @Override
    public void open(Configuration parameters) {
        allLocalSkylinePoints = getRuntimeContext().getListState(
                new ListStateDescriptor<>("allLocalSkylinePoints", DataPoint.class)
        );

        globalSkyline = getRuntimeContext().getListState(
                new ListStateDescriptor<>("globalSkyline", DataPoint.class)
        );

        totalReduceTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalReduceTime", Long.class)
        );

        reduceCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("reduceCount", Long.class)
        );

        totalMapTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("globalTotalMapTime", Long.class)
        );

        mapCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("globalMapCount", Long.class)
        );

        localSkylinesByPartition =
                getRuntimeContext().getMapState(
                        new MapStateDescriptor<>(
                                "localSkylinesByPartition",
                                Integer.class,
                                (Class<List<DataPoint>>) (Class<?>) List.class
                        )
                );

    }

    @Override
    public void processElement1(
            PartitionLocalSkyline value,
            Context ctx,
            Collector<String> out) throws Exception {

        localSkylinesByPartition.put(
                value.getPartitionId(),
                value.getSkyline()
        );

        long reduceStart = System.nanoTime();
        long t = totalMapTime.value() == null ? 0 : totalMapTime.value();
        long c = mapCount.value() == null ? 0 : mapCount.value();

        totalMapTime.update(t + value.getMapTimeNanos());
        mapCount.update(c + 1);


        List<DataPoint> current =
                StreamSupport.stream(globalSkyline.get().spliterator(), false)
                        .collect(Collectors.toList());

        for (DataPoint p : value.getSkyline()) {
            current = SkylineAlgorithms.BNL(current, p);
        }

        globalSkyline.update(current);

        // REDUCE stats
        long reduceEnd = System.nanoTime();
        totalReduceTime.update(
                (totalReduceTime.value() == null ? 0 : totalReduceTime.value())
                        + (reduceEnd - reduceStart)
        );
        reduceCount.update(
                (reduceCount.value() == null ? 0 : reduceCount.value()) + 1
        );
    }

    @Override
    public void processElement2(
            Integer query,
            Context ctx,
            Collector<String> out) throws Exception {

        long start = System.nanoTime();

        long mapTime = totalMapTime.value() == null ? 0L : totalMapTime.value();
        long mapCnt  = mapCount.value() == null ? 0L : mapCount.value();

        double avgMap =
                mapCnt == 0 ? 0.0 : (mapTime / (double) mapCnt) / 1_000_000.0;


        List<DataPoint> skyline =
                StreamSupport.stream(globalSkyline.get().spliterator(), false)
                        .collect(Collectors.collectingAndThen(
                                Collectors.toMap(
                                        DataPoint::getId,   // key = unique id
                                        p -> p,
                                        (p1, p2) -> p1      // keep first if duplicate
                                ),
                                m -> new ArrayList<>(m.values())
                        ));

        double optimalitySum = 0.0;
        int partitions = 0;

        for (Integer pid : localSkylinesByPartition.keys()) {
            List<DataPoint> local = localSkylinesByPartition.get(pid);

            if (local == null || local.isEmpty()) continue;

            long common =
                    local.stream()
                            .filter(skyline::contains)
                            .count();

            optimalitySum += common / (double) local.size();
            partitions++;
        }

        double avgLocalOptimality =
                partitions == 0 ? 0.0 : optimalitySum / partitions;


        long reduceTime = totalReduceTime.value() == null ? 0L : totalReduceTime.value();
        long reduceCnt  = reduceCount.value() == null ? 0L : reduceCount.value();

        double avgReduce =
                reduceCnt == 0 ? 0.0 : (reduceTime / (double) reduceCnt) / 1_000_000.0;


        // Emit to Kafka
        out.collect(skyline.toString());

        long end = System.nanoTime();
        MetricsPrinter.recordReduce(end - start);
        System.out.println("=== GLOBAL SKYLINE ===");
        skyline.forEach(System.out::println);
        MetricsPrinter.printStats();

    }
}
