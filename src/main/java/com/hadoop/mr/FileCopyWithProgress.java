package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * 将文件系统复制到hadoop文件系统
 
 */
public class FileCopyWithProgress {

    public static void main(String[] args) {

        //本地文件
        String localFile = "/Users/zhangchd/zhangchundi/d/soft/hadoop-2.7.3.tar.gz";

        //目的地址url
        String dst = "hdfs://127.0.0.1:9000/zhangchundi/hadoop-2.7.3.tar.gz";

        Configuration conf = new Configuration();

        try {
            InputStream in = new BufferedInputStream(new FileInputStream(localFile));

            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream out = fs.create(new Path(dst), new Progressable() {
                @Override
                public void progress() {
                    System.out.println("*");
                }
            });

            IOUtils.copyBytes(in, out, 4096, true);
            System.out.println("sucess!");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
