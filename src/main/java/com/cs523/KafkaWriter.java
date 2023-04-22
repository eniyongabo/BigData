package com.cs523;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class KafkaWriter {

    public static String OUTPUT_TOPIC = "electronic-analytics";

    Properties getProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void writeEvents(List<Tuple2<String, Integer>> events) {
        Producer<String, String> producer = new KafkaProducer<>(getProps());
        try {
            for (Tuple2<String, Integer> event : events) {
                producer.send(new ProducerRecord<String, String>(OUTPUT_TOPIC, event._1(), event._2().toString()));
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
