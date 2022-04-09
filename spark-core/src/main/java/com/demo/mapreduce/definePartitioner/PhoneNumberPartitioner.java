package com.demo.mapreduce.definePartitioner;

import com.demo.mapreduce.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @className: PhoneNumberPartitioner
 * @description: 根据电话号码进行分区
 *
 *
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/9
 */
public class PhoneNumberPartitioner extends Partitioner<Text, DefPartitionFlowBean> {


    @Override
    public int getPartition(Text text, DefPartitionFlowBean flowBean, int i) {
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
