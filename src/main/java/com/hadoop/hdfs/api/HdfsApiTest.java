package com.hadoop.hdfs.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * 文件上传下载
 * 
 * @author yongmaow
 *
 */
public class HdfsApiTest {
	Configuration conf = new Configuration();
	FileSystem fs;

	@Before
	public void confSet() throws IOException {

		conf.set("fs.defaultFS", "hdfs://172.16.89.69:9090");
		conf.set("dfs.replication", "1");
		fs = FileSystem.get(conf);
	}

	/**
	 * 上传
	 * 
	 * @throws IOException
	 */
	@Test
	public void putTest() throws IOException {

		System.setProperty("HADOOP_USER_NAME", "hadoop");

		// 上传
		fs.copyFromLocalFile(new Path("/home/yongmaow/ggg.txt"), new Path("/bb/ggg1.txt"));
		System.out.println("上传：/home/yongmaow/ggg.txt");

		// 下载
		fs.copyToLocalFile(new Path("/bb/ggg1.txt"), new Path("/home/yongmaow/ggg2.txt"));
		System.out.println("下载：/home/yongmaow/ggg2.txt");

		/**
		 * 上传和下载的API的底层封装其实就是 ： FileUtil.copy(....)
		 */

		fs.close();
	}

	/**
	 * 下载
	 * 
	 * @throws IOException
	 */
	@Test
	public void getTest() throws IOException {

		System.setProperty("HADOOP_USER_NAME", "hadoop");
		fs.copyToLocalFile(new Path("/bb/ggg1.txt"), new Path("/home/yongmaow/ggg4.txt"));
		System.out.println("下载：/home/yongmaow/ggg4.txt");

		fs.close();
	}

	/**
	 * 查看文件内容
	 * 
	 * @throws IOException
	 */
	@Test
	public void catTest() throws IOException {
		HdfsUtil.cat("/test-out/wordCount3/part-00000");
	}

	/**
	 * 复制本机文件至remote
	 * 
	 * @throws IOException
	 */
	@Test
	public void copyFileTest() throws IOException {
		HdfsUtil.copyFile("/home/yongmaow/ggg4.txt", "/test/a.txt");
	}

	@Test
	public void createFileTest() throws IOException {
		HdfsUtil.createFile("/test1/a.txt", "create file ");
	}

	@Test
	public void downloadTest() throws IOException {
		HdfsUtil.download("/test/a.txt", "/home/yongmaow/a.txt");
	}

	@Test
	public void lsTest() throws IOException {
		HdfsUtil.ls("/");
	}

	@Test
	public void mkdirsTest() throws IOException {
		HdfsUtil.mkdirs("/test1");
	}

	@Test
	public void renameTets() throws IOException {
		HdfsUtil.rename("/test/a.txt", "/test/ab.txt");
	}

	@Test
	public void rmrTest() throws IOException {
		HdfsUtil.rmr("/test1");
	}

	@Test
	public void deleteTest() throws IOException {
		boolean a = HdfsUtil.delete("/test1/a.txt", true);
		System.out.println(a);
	}

}