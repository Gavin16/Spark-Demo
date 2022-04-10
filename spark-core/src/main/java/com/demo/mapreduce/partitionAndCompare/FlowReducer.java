package com.demo.mapreduce.partitionAndCompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @className: FlowReducer
 * @description:
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/7
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
        for(Text value : values){
            context.write(value, key);
        }
    }
}
