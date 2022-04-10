package com.demo.mapreduce.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @className: LogReducer
 * @description: TODO
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/10
 */
public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        for(NullWritable nw : values){
            context.write(key, nw);
        }
    }
}
