package com.atguigu.mr2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author xuzl
 * @create 2019-07-28 7:31
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if(args.length<=0){
            //args= new String[]{"hdfs://hadoop-senior01.itguigu.com/input/words.txt","hdfs://hadoop-senior01.itguigu.com/output"};
            args = new String[]{"/input/words.txt","/output/"};
        }
        Tools.deleteFileInHDFS("/");
        WordCount wd = new WordCount();
        int status = ToolRunner.run(wd.getConf(),wd,args);
        System.exit(status);
    }
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job,inPath);

        job.setMapperClass(WordCountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;

    }
}
