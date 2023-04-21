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
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * TestHBase
 */
public class TestHBase {

    public static void main(String[] args) throws Exception {

        System.out.println("here 1");
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2182");

        System.out.println("here 2");
        // Create a connection to the HBase server
        Connection connection = ConnectionFactory.createConnection(config);

        System.out.println("here 3.1");

        // Create an admin object to perform table administration operations
        Admin admin = connection.getAdmin();

        System.out.println("here 3.2");

        // Create a table descriptor
        TableName tableName = TableName.valueOf("mytable-spark");
        TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("myfamily")).build();

        // Create the table
        admin.createTable(tableDesc);

        System.out.println("here 4");

        // Get a reference to the newly created table
        Table table = connection.getTable(tableName);

        // Use the table object to perform data operations

        // ...

        // Close the connection and table objects
        table.close();
        connection.close();

        // Configuration config = HBaseConfiguration.create();
        // config.set("hbase.master", "http://localhost:16010/");
        // config.set("hbase.zookeeper.quorum", "localhost");
        // config.set("hbase.zookeeper.property.clientPort", "2182");
        // TableName table1 = TableName.valueOf("TableSpark");
        // String family1 = "Family1";
        // String family2 = "Family2";
        // System.out.println("here 1");
        // Connection connection = ConnectionFactory.createConnection(config);
        // Admin admin = connection.getAdmin();

        // System.out.println("here 2");

        // HTableDescriptor desc = new HTableDescriptor(table1);
        // desc.addFamily(new HColumnDescriptor(family1));
        // desc.addFamily(new HColumnDescriptor(family2));
        // admin.createTable(desc);

        // byte[] row1 = Bytes.toBytes("row1");
        // Put p = new Put(row1);
        // p.addImmutable(family1.getBytes(), Bytes.toBytes("Qualifier1"),
        // Bytes.toBytes("cell_data"));

        // Table table = connection.getTable(table1);
        // System.out.println("DONNNNn");
        // table.put(p);
    }

}
