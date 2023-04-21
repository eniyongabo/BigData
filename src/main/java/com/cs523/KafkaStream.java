package com.cs523;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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
        initHBase();
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
            rdd.foreach(v -> {
                System.out.printf("-------%s, %s\n", v._1(), v._2());
                new HBaseWriter().write(time.toString(), v._1(), v._2().toString());
                // saveToHBase(time.toString(), v);
            });
            // System.out.println("Testing....");

            // testHBase();
        });

        ssc.start();
        ssc.awaitTermination();
    }

    static void initHBase() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        System.out.println("here 2");
        // Create a connection to the HBase server
        Connection connection = ConnectionFactory.createConnection(config);

        System.out.println("here 3.1");

        // Create an admin object to perform table administration operations
        Admin admin = connection.getAdmin();

        System.out.println("here 3.2");

        // Create a table descriptor
        TableName tableName = TableName.valueOf("electronic_store");
        TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family1")).build();

        // Create the table
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(tableDesc);
        System.out.println("here 4");

        // Get a reference to the newly created table
        // table = connection.getTable(tableName);

    }
}
