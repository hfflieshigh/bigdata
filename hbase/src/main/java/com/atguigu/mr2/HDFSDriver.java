package com.atguigu.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author xuzl
 * @create 2019-06-30 7:20
 */
public class HDFSDriver extends Configuration implements Tool {
    Configuration configuration = null;
    public int run(String[] strings) throws Exception {
        //获取job对象
        Job job = Job.getInstance(configuration);
        //设置主类
        job.setJarByClass(HDFSDriver.class);
        //设置Mapper
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);
        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit2",HDFSReducer.class,job);
        //设置输入路径
        FileInputFormat.setInputPaths(job,strings[0]);
        //提交
        boolean result = job.waitForCompletion(true);
        return result?0:1;
    }

    public void setConf(Configuration configuration) {
        configuration=configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int i=ToolRunner.run(configuration,new HDFSDriver(),args);
        System.exit(i);
    }
}
