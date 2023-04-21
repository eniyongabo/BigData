package com.jowilf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.shaded.org.eclipse.jetty.client.ProxyProtocolClientConnectionFactory.V2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
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
    public static void main(String[] args) throws Exception {

        Logger.getRootLogger().setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(5));
        ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka:9092");
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

        // counts.print();
        counts.foreachRDD((rdd, time) -> {
            rdd.foreach(v -> {
                System.out.printf("-------%s, %s\n", v._1(), v._2());
                saveToHBase(time.toString(), v);
            });
            // System.out.println("Testing....");

            // testHBase();
        });

        ssc.start();
        ssc.awaitTermination();
    }

    static void saveToHBase(String time, Tuple2<String, Integer> v) throws IOException {
        TableName tableName = TableName.valueOf("TableSpark");
        Configuration config = HBaseConfiguration.create();
        // config.set("hbase.master", "http://localhost:16010/");
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(config);
        Put p = new Put(Bytes.toBytes(time));
        p.addImmutable("family1".getBytes(), Bytes.toBytes(v._1()), Bytes.toBytes(v._2()));
        Table table = connection.getTable(tableName);
        table.put(p);
        
    }

    static void testHBase() throws IOException {
        TableName table1 = TableName.valueOf("TableSpark");
        String family1 = "Family1";
        String family2 = "Family2";
        System.out.println("here 1");
        Configuration config = HBaseConfiguration.create();
        // config.set("hbase.master", "http://localhost:16010/");
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        System.out.println("here 2");

        HTableDescriptor desc = new HTableDescriptor(table1);
        desc.addFamily(new HColumnDescriptor(family1));
        desc.addFamily(new HColumnDescriptor(family2));
        admin.createTable(desc);

        byte[] row1 = Bytes.toBytes("row1");
        Put p = new Put(row1);
        p.addImmutable(family1.getBytes(), Bytes.toBytes("Qualifier1"), Bytes.toBytes("cell_data"));

        Table table = connection.getTable(table1);
        System.out.println("DONNNNn");
        table.put(p);
    }
}
