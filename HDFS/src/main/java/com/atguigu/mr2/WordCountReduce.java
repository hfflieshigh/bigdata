package com.atguigu.mr2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xuzl
 * @create 2019-07-28 7:27
 */
public class WordCountReduce extends Reducer<Text, LongWritable,Text,LongWritable> {
    private LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        /**
         * <eilv ,[live,evil]>
         */
        int sum=0;
        for(LongWritable value : values){
            sum+=value.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
