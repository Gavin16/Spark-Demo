package com.demo.mapreduce.join.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @className: MapJoinDriver
 * @description:
 *
 * 使用cacheFile 环境存文件
 * 当需要对数据做join操作，数据中出现一个大表和一个小表时
 * 可以使用 map join 方式做关联；具体做法:
 * (1) 将小文件缓存起来，存储为K-V形式
 * (2) 遍历大文件时通过关联字段获取 小文件中的 V
 * (3) 将关联好的V 写入输出结果中
 *
 *
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/10
 */
public class MapJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException, URISyntaxException {

        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置加载jar包路径
        job.setJarByClass(MapJoinDriver.class);
        // 3 关联mapper
        job.setMapperClass(MapJoinMapper.class);
        // 4 设置Map输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 加载缓存数据
        job.addCacheFile(new URI("datas/pd.txt"));
        // Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("datas/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output222"));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
