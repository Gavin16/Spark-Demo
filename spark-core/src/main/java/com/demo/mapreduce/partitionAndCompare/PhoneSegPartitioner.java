package com.demo.mapreduce.partitionAndCompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @className: PhoneSegPartitioner
 * @description: 根据手机号段分区
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/9
 */
public class PhoneSegPartitioner extends Partitioner<FlowBean, Text> {


    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString();
        String phonePrefix = phone.substring(0, 3);

        int partitionId;
        if("136".equals(phonePrefix)){
            partitionId = 0;
        }else if("137".equals(phonePrefix)){
            partitionId = 1;
        }else if("138".equals(phonePrefix)){
            partitionId = 2;
        }else if("139".equals(phonePrefix)){
            partitionId = 3;
        }else{
            partitionId = 4;
        }
        return partitionId;
    }

}
