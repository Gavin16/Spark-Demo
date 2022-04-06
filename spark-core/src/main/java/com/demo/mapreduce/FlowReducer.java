package com.demo.mapreduce;

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
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {

        Long upFlowTotal = 0L;
        Long downFlowTotal = 0L;
        Long sumFlowTotal = 0L;

        for(FlowBean bean : values){
            upFlowTotal += bean.getUpFlow();
            downFlowTotal += bean.getDownFlow();
            sumFlowTotal += bean.getSumFlow();
        }

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlowTotal);
        flowBean.setDownFlow(downFlowTotal);
        flowBean.setSumFlow(sumFlowTotal);

        context.write(key, flowBean);
    }
}
