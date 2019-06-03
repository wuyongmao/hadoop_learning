package com.hadoop.mr;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * 使用hadoop-url读取文件
 */
public class UrlCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {


        String input = "hdfs://172.16.89.69:9090/bb/ggg1.txt";

        InputStream in = null;

        try {

            in = new URL(input).openStream();

            IOUtils.copyBytes(in, System.out, 4096, false);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);

        }
    }

}
