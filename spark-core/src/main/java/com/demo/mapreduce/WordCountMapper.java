package com.demo.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // map 方法对每对 k-v 执行一次， 定义为全局属性可以减少内存消耗
    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\W");

        for(String word : words) {
            if(StringUtils.isBlank(word)) continue;
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }



}
