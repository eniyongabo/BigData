package com.cs523;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

import scala.Tuple2;

public class KafkaStream {

    public static void main(String[] args) throws InterruptedException, IOException {
        TableUtils tableUtils = new TableUtils();
        tableUtils.createTable();
        Logger.getRootLogger().setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaNetworkWordCount");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka:29092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        List<String> topics = Arrays.asList("electronic_store");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaPairDStream<String, Integer> counts = stream.map(record -> record.value().toString())
                .mapToPair((String line) -> {
                    return new Tuple2<String, Integer>(line.split(",")[1], 1);
                }).reduceByKey((x, y) -> x + y);

        // counts.print();
        counts.foreachRDD((rdd, time) -> {
            rdd.foreachPartition(events -> {
                HBaseWriter writer = new HBaseWriter();
                while (events.hasNext()) {
                    Tuple2<String, Integer> event = events.next();
                    writer.write(time.toString(), event._1(), event._2().toString());
                }
                writer.close();
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }

}
