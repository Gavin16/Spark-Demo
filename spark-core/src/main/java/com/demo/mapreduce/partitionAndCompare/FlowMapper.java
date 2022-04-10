package com.demo.mapreduce.partitionAndCompare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @className: FlowMapper
 * @description:
 *
 * 数据输入格式为:
 * 13630577991	6960	690	7650
 *
 *
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/7
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text outVal = new Text();
    private FlowBean outKey = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        String phone = split[0];
        Long upFlow = Long.parseLong(split[split.length - 3]);
        Long downFlow = Long.parseLong(split[split.length - 2]);

        outKey.setUpFlow(upFlow);
        outKey.setDownFlow(downFlow);
        outKey.setSumFlow();
        outVal.set(phone);

        context.write(outKey,outVal);
    }
}
