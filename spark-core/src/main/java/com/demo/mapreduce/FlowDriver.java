package com.demo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @className: FlowDriver
 * @description:
 *
 * 统计用户上下行流量及总流量
 *
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/7
 */
public class FlowDriver {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("datas/hadoop/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output4"));

        boolean res = job.waitForCompletion(Boolean.TRUE);
        System.exit(res ? 0 : 1);
    }
}
