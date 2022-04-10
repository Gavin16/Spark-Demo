package com.demo.mapreduce.join.mapJoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @className: MapJoinMapper
 * @description:
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/10
 */
public class MapJoinMapper  extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while(StringUtils.isNotBlank(line = bufferedReader.readLine())){
            String[] split = line.split(" ");
            pdMap.put(split[0], split[1]);
        }
        IOUtils.closeStream(fis);
    }

    private Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        // 获取pid
        String pdName = pdMap.get(split[1]);
        // 获取订单ID 和 订单数量
        outK.set(split[0] + "\t" + pdName + "\t" + split[2]);
        context.write(outK, NullWritable.get());
    }
}
