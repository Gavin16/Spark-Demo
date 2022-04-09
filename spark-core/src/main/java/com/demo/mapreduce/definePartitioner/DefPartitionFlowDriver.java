package com.demo.mapreduce.definePartitioner;

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
 * 自定义分区 partition 分区器
 * 同事需要设置 ReduceTask 数量
 *
 * @version: 1.0
 * @author: minsky
 * @date: 2022年04月09日
 */
public class DefPartitionFlowDriver {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(DefPartitionFlowDriver.class);

        job.setMapperClass(DefPartitionFlowMapper.class);
        job.setReducerClass(DefPartitionFlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DefPartitionFlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DefPartitionFlowBean.class);
        // 文本输入格式化选择，默认 TextInputFormat
        // 若输入文件中存在大量小文件，则使用 CombineTextInputFormat
        job.setInputFormatClass(TextInputFormat.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(PhoneNumberPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path("datas/hadoop/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output5"));

        boolean res = job.waitForCompletion(Boolean.TRUE);
        System.exit(res ? 0 : 1);
    }
}
