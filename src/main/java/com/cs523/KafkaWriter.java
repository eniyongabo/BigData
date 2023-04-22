package com.cs523;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode actualObj = mapper.createObjectNode();
            for (Tuple2<String, Integer> event : events) {
                actualObj.put(event._1(), event._2());
            }
            producer.send(new ProducerRecord<String, String>(OUTPUT_TOPIC, "event_type_agg", actualObj.toString()));
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
