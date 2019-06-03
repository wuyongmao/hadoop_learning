package com.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest {

	Configuration conf = HBaseConfiguration.create();
	HBaseHelper helper;
	Connection connection;

	@Before
	public void setConf() throws IOException {
		conf.set("zookeeper.znode.parent", "/hbase");
		conf.set("hbase.zookeeper.quorum", "172.16.89.69");
		conf.set("hive.zookeeper.client.port", "2181");
		helper = HBaseHelper.getHelper(conf);
		connection = ConnectionFactory.createConnection(conf);
	}

	@Test
	public void testPut() throws IOException {

		helper.dropTable("testtable3");
		helper.createTable("testtable3", "colfam1");
		Table table = connection.getTable(TableName.valueOf("testtable3"));
		Put put = new Put(Bytes.toBytes("row1"));

		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

		table.put(put);
		table.close();
		connection.close();

		helper.close();

	}

	@Test
	public void testScan() throws IOException {
		
		System.out.println(helper.existsTable("testtable3"));
//		Table table = connection.getTable(TableName.valueOf("testtable3"));
		 
		
	}

}
