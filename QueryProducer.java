package tuc_stdb_flink_project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;

public class QueryProducer {

    // query sleeping time (default: 1 sec)
    public static long sleeping_time = 1000;

    // number of queries to be sent
    // Every 10 tuples, one query sent
    // (with some added delay between the query sends)
    public static int Q = DataPointProducer.N / 10;

    public static void main(String[] args) throws Exception {
        // Query random
        Random qrnd = new Random();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        /* Send one query every query_freq milliseconds
           In order to achieve non-uniform query sending,
           in the main function, the integers sent FIX!!!!
        * */
        for (int i = 0; i < Q; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("queries-topic", String.valueOf(i));
            producer.send(record);
            System.out.println("Sent query " + i);

            // Pick uniformly a random sleeping time in [0.5, 3] (500 - 3000 ms) seconds
            sleeping_time = 3000 + qrnd.nextInt(5000 - 3000 + 1);
            Thread.sleep(sleeping_time);
        }

        producer.close();
    }
}

