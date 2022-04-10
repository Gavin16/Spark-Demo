package com.demo.mapreduce.outputFormat;

import com.demo.mapreduce.FlowBean;
import com.demo.mapreduce.FlowDriver;
import com.demo.mapreduce.FlowMapper;
import com.demo.mapreduce.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @className: LogDriver
 * @description:
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/10
 */
public class LogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 文本输入格式化选择，默认 TextInputFormat
        // 若输入文件中存在大量小文件，则使用 CombineTextInputFormat
        job.setInputFormatClass(TextInputFormat.class);

        // FileOutputFormat 需要输出 _SUCCESS文件, 因此还需要额外通过
        // FileOutputFormat 设置 outputPath
        job.setOutputFormatClass(LogOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("datas/logs.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output111"));

        boolean res = job.waitForCompletion(Boolean.TRUE);
        System.exit(res ? 0 : 1);
    }
}
