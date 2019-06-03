package com.hadoop.mr;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class hdfs {

    public static void main(String[] args) throws MalformedURLException {
        /*URL url=new URL("http://www.baidu.com");//用URL判断路径
        try {
            InputStream in=url.openStream();//路径打开一个输入流文件
             IOUtils.copyBytes(in, System.out, 4096,true);//将文件内容拷贝出来
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
        
        /*
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());//用URL的方法先建立一个url的工厂
        URL url=new URL("hdfs://172.16.89.69:9000/hello.txt");//获取我们想要文件的路径
        try {
            InputStream in=url.openStream();//文件打开
             IOUtils.copyBytes(in, System.out, 4096,true);//文件内容输出
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
        
        
        
        Configuration conf =new Configuration();//configuration的对象建立
        conf.set("fs.defaultFS", "hdfs://172.16.89.69:9090");//将要访问的路径放入
        try {
            FileSystem fileSystem=FileSystem.get(conf);//获取路径信息
            boolean success=fileSystem.mkdirs(new Path("/asb"));//判断是否创建一个目录文件
            System.out.println(success);
            
            /*boolean success=fileSystem.mkdirs(new Path("/asb"));//判断是否创建一个目录文件
            System.out.println(success);
            
            success=fileSystem.exists(new Path("\\hello.txt"));//判断是否存在文件
            System.out.println(success);
            
            success=fileSystem.delete(new Path("/asb"));//删除文件
            System.out.println(success);
            
            success=fileSystem.exists(new Path("/asb"));
            System.out.println(success);*/
            
            /*FSDataOutputStream out =fileSystem.create(new Path("/test.data"),true);//重新创建一个data目录
            FileInputStream fis =new FileInputStream("C:\\Users\\ZB\\Desktop\\data\\Hamlet.txt");//将桌面文件读入
            IOUtils.copyBytes(fis, out, 4096,true);*///读取放入的文件
            
            /*FSDataOutputStream out =fileSystem.create(new Path("/test.data"),true);//手动添加
            FileInputStream fis =new FileInputStream("C:\\Users\\ZB\\Desktop\\data\\Hamlet.txt");
            byte[] buf=new byte[4096];//一个字节一个字节的上传
            int len=fis.read(buf);
            while(len!=-1) {
                out.write(buf,0,len);
                len=fis.read(buf);
            }
            fis.close();
            out.close();*/
            
            FileStatus[] status=fileSystem.listStatus(new Path("/"));//查找存在根目录下的文件
            for(FileStatus status2 : status) {
                System.out.println(status2.getPath());//获取地址
                System.out.println(status2.getPermission());//获取认证信息
                System.out.println(status2.getReplication());//获取响应
                
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
         
        
        
                
            
    }
}