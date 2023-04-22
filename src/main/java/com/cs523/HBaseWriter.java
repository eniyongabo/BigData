package com.cs523;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseWriter {
    private Table table;
    private Connection connection;
    TableUtils tableUtils = new TableUtils();

    public HBaseWriter() throws IOException {
        connection = tableUtils.newConnection();
        table = connection.getTable(TableUtils.TABLE);
    }

    void write(String time, String key, String value) throws IOException {
        Put p = new Put(Bytes.toBytes(time));
        p.addImmutable(TableUtils.CF_EVENTS.getBytes(), Bytes.toBytes(key), Bytes.toBytes(value));
        table.put(p);
    }

    public void close() throws IOException {
        table.close();
    }
}
