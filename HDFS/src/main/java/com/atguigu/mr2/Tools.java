package com.atguigu.mr2;


import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * @author xuzl
 * @create 2019-07-28 7:42
 */
public class Tools {
    static Configuration conf = new Configuration(true);

//    public static void main(String[] args) {
//        getDirectoryFromHDFS("/input");
//        uploadFile2HDFS("/home/z/Desktop/demo.txt","/input/");
//        getDirectoryFromHDFS("/input");
//    }

    //向HDFS中上传文件
    public static void uploadFile2HDFS(String src,String dst) throws IOException{
        FileSystem fileSystem = FileSystem.get(conf);
        Path srcPath = new Path(src);
        Path dstPath = new Path(src);

        long startTime =System.currentTimeMillis();
        fileSystem.copyFromLocalFile(false,srcPath,dstPath);
        long endTime=System.currentTimeMillis();
        System.out.println("耗时："+(endTime-startTime)+"ms");
        fileSystem.close();
    }

    //输出HDFS中的文件到控制台
    public static void showFileFromHDFS(String src)throws IOException{
        //获取文件系统管理对象
        FileSystem fileSystem =FileSystem.get(conf);
        //得到一个输入路径
        Path inPath = new Path(src);
        //通过该路径生成输入流对象
        FSDataInputStream fsDataInputStream =fileSystem.open(inPath);
        //打印hdfs系统中的words.txt文档到控制台
        IOUtils.copyBytes(fsDataInputStream,System.out,1024,false);
        //关闭流
        IOUtils.closeStream(fsDataInputStream);
    }
    //删除指定文件或目录
    public static void deleteFileInHDFS(String directory)throws IOException{
        //获取文件系统管理对象
        FileSystem fileSystem =FileSystem.get(URI.create(directory),conf);
        FileStatus [] fileList =fileSystem.listStatus(new Path(directory));
        for(int i=0;i<fileList.length;i++){
            FileStatus fileStatus = fileList[i];
            if(fileStatus.getPath().getName().startsWith("output")){
                fileSystem.delete(fileStatus.getPath(),true);
            }
        }
    }

    //打印指定目录下的所有文件
    public static void getDirectoryFromHDFS(String directory)throws IOException{
        FileSystem fileSystem = FileSystem.get(URI.create(directory), conf);
        FileStatus [] fileList =fileSystem.listStatus(new Path(directory));
        System.out.println("___________________********************________________");
        for(int i=0;i<fileList.length;i++){
            FileStatus fileStatus = fileList[i];
            System.out.println("Name:"+fileStatus.getPath().getName());
            System.out.println("Size:"+fileStatus.getLen());
        }
        System.out.println("_________________********************________________");
        fileSystem.close();
    }



























}
