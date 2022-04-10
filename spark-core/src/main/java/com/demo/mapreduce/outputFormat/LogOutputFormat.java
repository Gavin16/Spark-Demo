package com.demo.mapreduce.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @className: LogOutputFormat
 * @description: TODO
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/10
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        LogRecordWriter lrw = new LogRecordWriter(taskAttemptContext);
        return lrw;
    }
}

