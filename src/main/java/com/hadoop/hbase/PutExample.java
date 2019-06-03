package com.hadoop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

 
public class PutExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
//        conf.addResource("hbase-site.xml");
         conf.set("zookeeper.znode.parent", "/hbase");
          conf.set("hbase.zookeeper.quorum", "172.16.89.69");
           conf.set("hive.zookeeper.client.port", "2181");
//
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable2");
        helper.createTable("testtable2", "colfam1");


        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable2"));

        Put put = new Put(Bytes.toBytes("row1"));

        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                Bytes.toBytes("val1"));

        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                Bytes.toBytes("val2"));

        table.put(put);
        table.close();
        connection.close();

        helper.close();
    }
}