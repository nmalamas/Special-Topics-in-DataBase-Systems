/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tuc_stdb_flink_project;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class SkylineMain {

    // MR Skyline Query Processing Algorithm chosen (MR-Dim | MR-Grid | MR-Angle)
    public static String algorithm = "MR-Angle"; //  get from CLI if possible args[0];

    // Level of parallelism
    public static int par = 8; // get from CLI if possible Integer.parseInt(args[1]);
    public static ObjectMapper oMapper = new ObjectMapper();

	public static void main(String[] args) throws Exception {

        // Sets up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);

        // Set the Source Kafka Topic for the Tuple Stream
        KafkaSource<DataPoint> tuplesSource = KafkaSource.<DataPoint>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("tuples-topic")
                .setStartingOffsets(OffsetsInitializer.latest()) //check
                .setGroupId("tuples-group")
                .setValueOnlyDeserializer(new DataPointJsonDeserializationSchema())
                .build();

        DataStream<DataPoint> tuplesStream =
                env.fromSource(
                                tuplesSource,
                                WatermarkStrategy.noWatermarks(),
                                "tuples"
                        )
                        .map(p -> {
                            p.setIngestTimestamp(System.nanoTime());
                            return p;
                        });


        // Set the Source Kafka Topic for the Query Stream
        KafkaSource<String> queriesSource =
                KafkaSource.<String>builder()
                        //.setProperties(props) //check
                        .setBootstrapServers("localhost:9092")
                        .setTopics("queries-topic")
                        .setStartingOffsets(OffsetsInitializer.latest()) //check
                        .setGroupId("queries-group")
                        .setValueOnlyDeserializer(new SimpleStringSchema())  //check
                        .build();

        DataStream<Integer> queriesStream =
                env.fromSource(
                        queriesSource,
                        WatermarkStrategy.noWatermarks(),
                        "queries"
                ).map(x -> 1)
                 .keyBy(x -> x); //make the queries stream keyed for later connection with the partitined tuples stream

        DataStream<DataPoint> mappedTuples;

        switch(algorithm) {
            case "MR-Dim":
                // Partition tuples according to MR-Dim
                mappedTuples =
                        tuplesStream.map(p -> {
                            long ingestionEnd = System.nanoTime();
                            MetricsPrinter.recordIngestion(
                                    ingestionEnd - p.getIngestTimestamp()
                            );

                            long mapStart = System.nanoTime();
                            int pid = SkylineAlgorithms.MRDimPartition(p, 0, par, DataPointProducer.max_intervals[0]);
                            long mapEnd = System.nanoTime();

                            MetricsPrinter.recordMap(mapEnd - mapStart);


                            p.setPartitionId(pid);
                            p.setMapTimeNanos(mapEnd - mapStart);

                            return p;
                        });
                break;
            case "MR-Grid":
                // Partition tuples according to MR-Grid
                mappedTuples =
                        tuplesStream.map(p -> {
                            long ingestionEnd = System.nanoTime();
                            MetricsPrinter.recordIngestion(
                                    ingestionEnd - p.getIngestTimestamp()
                            );

                            long mapStart = System.nanoTime();
                            int pid = SkylineAlgorithms.MRGridPartition(p, DataPointProducer.max_intervals);
                            long mapEnd = System.nanoTime();

                            MetricsPrinter.recordMap(mapEnd - mapStart);


                            p.setPartitionId(pid);
                            p.setMapTimeNanos(mapEnd - mapStart);

                            return p;
                        });
                break;
            case "MR-Angle":
                // Partition tuples according to MR-Angle
                mappedTuples =
                        tuplesStream.map(p -> {
                            long ingestionEnd = System.nanoTime();
                            MetricsPrinter.recordIngestion(
                                    ingestionEnd - p.getIngestTimestamp()
                            );

                            long mapStart = System.nanoTime();
                            int pid = SkylineAlgorithms.MRAnglePartition(p, par);
                            long mapEnd = System.nanoTime();

                            MetricsPrinter.recordMap(mapEnd - mapStart);


                            p.setPartitionId(pid);
                            p.setMapTimeNanos(mapEnd - mapStart);

                            return p;
                        });
                break;
            default:
                mappedTuples = null;
        }

        KeyedStream<DataPoint, Integer> partitionedTuples =
                mappedTuples.keyBy(DataPoint::getPartitionId);

        // Stream the localSkylines, as recently computed
        DataStream<PartitionLocalSkyline> localSkylineStream =
                partitionedTuples
                        .process(new LocalSkylineComputationFunction());

        // Key the two streams into the same constant key
        KeyedStream<PartitionLocalSkyline, Integer> keyedLocal =
                localSkylineStream.keyBy(x -> 0);

        KeyedStream<Integer, Integer> keyedQueries =
                queriesStream.keyBy(x -> 0);

        // Connect the two Keyed Streams
        DataStream<String> globalSkylineStream =
                keyedLocal
                        .connect(keyedQueries)
                        .process(new GlobalSkylineComputationFunction());

        // Set the Sink Kafka Topic for the Current Total Skyline (CTS)
        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("cts-topic")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build();

        globalSkylineStream.sinkTo(sink);

        // Execute program, beginning computation.
		env.execute("STDB Flink Project");
	}
}
