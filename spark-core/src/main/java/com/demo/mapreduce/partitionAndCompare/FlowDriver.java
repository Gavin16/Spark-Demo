package com.demo.mapreduce.partitionAndCompare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @className: FlowDriver
 * @description:
 *
 * 根据手机号区分文件，并让总流量降序排序，若总流量相同则按照上行流量升序排序
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

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 文本输入格式化选择，默认 TextInputFormat
        // 若输入文件中存在大量小文件，则使用 CombineTextInputFormat
        job.setInputFormatClass(TextInputFormat.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(PhoneSegPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path("output4"));
        FileOutputFormat.setOutputPath(job, new Path("output7"));

        boolean res = job.waitForCompletion(Boolean.TRUE);
        System.exit(res ? 0 : 1);
    }
}
