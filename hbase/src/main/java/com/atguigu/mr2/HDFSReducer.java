package com.atguigu.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author xuzl
 * @create 2019-06-30 7:27
 */
public class HDFSReducer extends TableReducer<NullWritable, Put,NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //遍历写出
        for (Put value : values){
            context.write(NullWritable.get(),value);
        }
    }
}
