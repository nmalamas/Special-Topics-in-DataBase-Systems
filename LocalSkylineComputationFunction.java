package tuc_stdb_flink_project;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

public class LocalSkylineComputationFunction extends KeyedProcessFunction<Integer, DataPoint, PartitionLocalSkyline> {

        private transient ListState<DataPoint> skylineState;

        private transient org.apache.flink.api.common.state.ValueState<Long> totalMapTime;
        private transient org.apache.flink.api.common.state.ValueState<Long> mapCount;

        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<DataPoint> desc =
                    new ListStateDescriptor<>(
                            "localSkyline",
                            DataPoint.class
                    );
            skylineState = getRuntimeContext().getListState(desc);

            totalMapTime = getRuntimeContext().getState(
                    new org.apache.flink.api.common.state.ValueStateDescriptor<>("totalMapTime", Long.class)
            );

            mapCount = getRuntimeContext().getState(
                    new org.apache.flink.api.common.state.ValueStateDescriptor<>("mapCount", Long.class)
            );

        }

        // Local Skyline computation via BNL
        @Override
        public void processElement(
                DataPoint p,
                Context ctx,
                Collector<PartitionLocalSkyline> out) throws Exception {

            long currentTotal =
                    totalMapTime.value() == null ? 0 : totalMapTime.value();
            long currentCount =
                    mapCount.value() == null ? 0 : mapCount.value();

            totalMapTime.update(currentTotal + p.getMapTimeNanos());
            mapCount.update(currentCount + 1);


            List<DataPoint> skyline = new ArrayList<>();
            for (DataPoint s : skylineState.get()) {
                skyline.add(s);
            }

            // Used for the MRGrid algorithm:
            // If the point is in the top right partition
            // then for sure it is not contained in the global skyline
            // So points that may fall in this partition
            // not considered to be candidates at all (they have partition_id = -1)
            if (p.getPartitionId() != -1) {
                skyline = SkylineAlgorithms.BNL(skyline, p);
                skylineState.update(skyline);
            }

            out.collect(
                    new PartitionLocalSkyline(
                            ctx.getCurrentKey(),
                            new ArrayList<>(skyline),
                            totalMapTime.value()
                    )
            );
        }
}
