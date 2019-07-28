package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**map阶段
 * KEYIN 输入数据的key
 * VALUE 输入数据的value
 * KEYOUT 输出数据的key的类型 atguigu,1 ss,1
 * KEYOUT 输出数据的value类型
 * @author xuzl
 * @create 2019-07-06 11:28
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取一行
        String line = value.toString();
        //2、切割单词
        String[] words = line.split(" ");
        //3、循环写出
        for(String word : words){
            //atguigu
            k.set(word);
            context.write(k,v);

        }
    }
}
