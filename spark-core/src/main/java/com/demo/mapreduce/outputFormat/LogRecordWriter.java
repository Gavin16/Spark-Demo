package com.demo.mapreduce.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @className: LogRecordWriter
 * @description:
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/10
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream bdi;
    private FSDataOutputStream others;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            bdi = fs.create(new Path("output111/gbabdri.txt"));
            others = fs.create(new Path("output111/others.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String url = text.toString();
        if(url.contains("gbabdri")){
            bdi.writeBytes(url + "\n");
        }else{
            others.writeBytes(url + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(bdi);
        IOUtils.closeStream(others);
    }
}
