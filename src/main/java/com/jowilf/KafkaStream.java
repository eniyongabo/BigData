package com.jowilf;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class KafkaStream {
    public static void main(String[] args) throws InterruptedException {
        Logger.getRootLogger().setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("electronic_store");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaPairDStream<String, Integer> counts = stream.map(record -> record.value().toString())
                .mapToPair((String line) -> {
                    JsonNode actualObj = mapper.readTree(line);
                    return new Tuple2<String, Integer>(actualObj.get("event_type").asText(), 1);
                }).reduceByKey((x, y) -> x + y);

        counts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
