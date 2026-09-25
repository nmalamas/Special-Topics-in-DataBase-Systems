package tuc_stdb_flink_project;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class DataPointProducer {
    public static int N = 20_000_000;
    public static int dims = 2;
    public static int[] max_intervals = {1000, 1000};
    public static long rnd_seed = 42;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random rnd = new Random(rnd_seed);

        for (int i = 0; i < N; i++) {
            double[] coords = correlatedData(dims, rnd, max_intervals);
            DataPoint p = new DataPoint(i, coords);

            byte[] json = mapper.writeValueAsBytes(p);
            producer.send(new ProducerRecord<>("tuples-topic", String.valueOf(i), json));
            if (i % 100_000 == 0) System.out.println("Sent " + i);
        }


        producer.close();
        System.out.println("DONE");
    }

    // Anti correlated data generator
    private static double[] antiCorData(int dims, Random rnd, int[] max_intervals) {
        double x = rnd.nextDouble() * max_intervals[0];

        // Anti-correlation + controlled noise
        double noiseStd = 0.05 * max_intervals[1];   // 5% of range (tune if needed)
        double y = (max_intervals[1] - x) + noiseStd * rnd.nextGaussian();

        // Clamp y to (0, max_intervals[1])
        y = Math.max(0.0, Math.min(y, max_intervals[1]));

        return new double[]{x, y};
    }

    // Correlated data generator
    private static double[] correlatedData(int dims, Random rnd, int[] max_intervals) {
        double[] coords = new double[dims];

        // Choose a base value uniformly
        double base = rnd.nextDouble();

        for (int d = 0; d < dims; d++) {
            // Strong correlation: all dimensions follow base
            double value = base * max_intervals[d];

            // Add small uniform noise (NOT Gaussian)
            double noise = (rnd.nextDouble() - 0.5) * 0.1 * max_intervals[d];

            value += noise;

            // Clamp safely
            value = Math.max(0.0, Math.min(value, max_intervals[d]));
            coords[d] = value;
        }

        return coords;
    }

    // Uniformly distributed data generator
    private static double[] uniformData(int dims, Random rnd, int[] max_intervals) {
        double[] coords = new double[dims];

        for (int d = 0; d < dims; d++) {
            coords[d] = rnd.nextDouble() * max_intervals[d];
        }

        return coords;
    }


}
