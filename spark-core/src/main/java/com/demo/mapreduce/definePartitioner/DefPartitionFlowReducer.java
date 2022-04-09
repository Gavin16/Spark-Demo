package com.demo.mapreduce.definePartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @className: FlowReducer
 * @description:
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/7
 */
public class DefPartitionFlowReducer extends Reducer<Text, DefPartitionFlowBean, Text, DefPartitionFlowBean> {

    @Override
    protected void reduce(Text key, Iterable<DefPartitionFlowBean> values, Reducer<Text, DefPartitionFlowBean, Text, DefPartitionFlowBean>.Context context)
            throws IOException, InterruptedException {

        Long upFlowTotal = 0L;
        Long downFlowTotal = 0L;
        Long sumFlowTotal = 0L;

        for(DefPartitionFlowBean bean : values){
            upFlowTotal += bean.getUpFlow();
            downFlowTotal += bean.getDownFlow();
            sumFlowTotal += bean.getSumFlow();
        }

        DefPartitionFlowBean flowBean = new DefPartitionFlowBean();
        flowBean.setUpFlow(upFlowTotal);
        flowBean.setDownFlow(downFlowTotal);
        flowBean.setSumFlow(sumFlowTotal);

        context.write(key, flowBean);
    }
}
