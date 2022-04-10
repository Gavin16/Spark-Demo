package com.demo.mapreduce.combiner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 创建执行的job
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        // 设置当前jar包路径
        job.setJarByClass(WordCountDriver.class);
        // 设置当前jar包路径
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 设置mapper 输出的k-v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置最终输出的k-v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置combiner: 在mapper输出时运行减少reduce 工作压力
        // 使用combiner前提条件: 不影响业务结果(由于是在Mapper阶段输出时进行处理,可能导致局部处理影响整体结果)
        job.setCombinerClass(WordCountReducer.class);

        // 设置输入路径和输出路径
        // 输入输出路径使用 HDFS 文件路径
        FileInputFormat.setInputPaths(job,new Path("datas/hello.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output8"));
        // 提交job
        boolean result = job.waitForCompletion(Boolean.TRUE);
        System.exit(result? 0: 1);
    }

}
