package com.demo.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @className: FlowMapper
 * @description: TODO
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/7
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outVal = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split("\t");

        String phone = split[1];
        Long upFlow = Long.parseLong(split[split.length - 3]);
        Long downFlow = Long.parseLong(split[split.length - 2]);

        outK.set(phone);
        outVal.setUpFlow(upFlow);
        outVal.setDownFlow(downFlow);
        outVal.setSumFlow();

        context.write(outK,outVal);
    }
}
