package com.cs523;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseWriter {
    private Table table;
    private Connection connection;

    public HBaseWriter() throws IOException {
        initConnection();
    }

    void initConnection() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf("electronic_store");
        table = connection.getTable(tableName);
    }

    void write(String time, String key, String value) throws IOException {
        Put p = new Put(Bytes.toBytes(time));
        p.addImmutable("family1".getBytes(), Bytes.toBytes(key), Bytes.toBytes(value));
        table.put(p);
        table.close();
    }
}
