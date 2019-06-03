package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * 显示Hadoop文件系统的一个目录的文件信息
  
 */
public class ListStatus {

    public static void main(String[] args) {
       // listStatus();

        String uri = "hdfs://127.0.0.1:9000/zhangchundi";
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            FileStatus[] status = fs.listStatus(new Path(URI.create(uri)),
                    new RegexExcludePathFilter(".*\\.txt")); //注意: 留下不匹配的文件
            Path[] listPaths = FileUtil.stat2Paths(status);
            for (Path path : listPaths) {
                System.out.println(path);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @SuppressWarnings("unused")
	private static void listStatus() {
        //目的地址uri
        String uri = "hdfs://127.0.0.1:9000/zhangchundi";
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);

            Path path = new Path(uri);
            if (fs.exists(path)) {
                for (FileStatus status : fs.listStatus(path)) {
                    System.out.println(status.getPath());
                }
            }

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
