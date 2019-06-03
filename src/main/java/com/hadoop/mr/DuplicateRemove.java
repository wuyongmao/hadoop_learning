package com.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * hadoop 数据去重
 * <p>
 */
public class DuplicateRemove {


    public static class DuplicateRemoveMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

        private Text word = new Text();


        @Override
        public void map(Object longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String value = text.toString(); //将输入的纯文本文件的数据转化成String
            System.out.println(value + "*********");

            word.set(value);

            outputCollector.collect(word, new Text(""));

        }
    }


    public static class DuplicateRemoveReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws
                IOException {

            output.collect(key, new Text(""));

        }


    }


    public static void main(String[] args) throws Exception {

        String input = "hdfs://172.16.89.69:9090/test/";

        String output = "hdfs://172.16.89.69:9090/test-out/wordCount3";


        JobConf conf = new JobConf(DuplicateRemove.class);

        conf.setJobName("DuplicateRemove"); //设置一个用户定义的job名称

//        conf.addResource("classpath:/hadoop/core-site.xml");
//
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//
//        conf.addResource("classpath:/hadoop/mapred-site.xml");


        conf.setOutputKeyClass(Text.class);//为job的输出数据设置Key类

        conf.setOutputValueClass(Text.class); //为job输出设置value类


        conf.setMapperClass(DuplicateRemove.DuplicateRemoveMapper.class); //为job设置Mapper类

        conf.setCombinerClass(DuplicateRemove.DuplicateRemoveReducer.class);  //为job设置Combiner类

        conf.setReducerClass(DuplicateRemove.DuplicateRemoveReducer.class); //为job设置Reduce类


        conf.setInputFormat(TextInputFormat.class);//为map-reduce任务设置InputFormat实现类

        conf.setOutputFormat(TextOutputFormat.class);//为map-reduce任务设置OutputFormat实现类

        /**
         * InputFormat描述map-reduce中对job的输入定义
         * setInputPaths():为map-reduce job设置路径数组作为输入列表
         * setInputPath()：为map-reduce job设置路径数组作为输出列表
         */
        FileInputFormat.setInputPaths(conf, new Path(input));

        FileOutputFormat.setOutputPath(conf, new Path(output));


        //运行一个job
        JobClient.runJob(conf);

        System.exit(0);

    }
}
