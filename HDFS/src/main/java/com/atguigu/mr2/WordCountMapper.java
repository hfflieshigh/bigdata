package com.atguigu.mr2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xuzl
 * @create 2019-07-28 7:21
 * 查找变位词
 * 例如：live evil
 */
public class WordCountMapper extends Mapper<LongWritable,Text, Text,LongWritable> {
    private final LongWritable one = new LongWritable();
    private final Text key = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineValue = value.toString();
        //1、读取每一行单词
        //2、分割单词
        //3、排序当前单词作为map输出的key
        //4、当前单词(没有经过排序加工)作为value写入到context中
        String [] values =lineValue.split("\t");
        for(String tmpKey : values){
            this.key.set(tmpKey);
            context.write(this.key,one);
        }
    }
}
