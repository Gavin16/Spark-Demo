package com.demo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @className: HadoopApiClientTest
 * @description: 测试API连接Hadoop 进行CRUD
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/3
 */
public class HadoopApiClientTest {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://192.168.0.118:18020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        // 用户
        String user = "root";

        // 1 获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }


    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        fs.mkdirs(new Path("/wcinput"));
    }

    @Test
    public void testFilePut() throws IOException {
        fs.copyFromLocalFile(false , true, new Path("/Users/minsky/Develop/hadoop/wc/words.txt"),
                new Path("hdfs://192.168.0.118:18020/input"));
    }

}
