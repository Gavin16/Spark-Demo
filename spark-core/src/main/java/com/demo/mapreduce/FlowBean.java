package com.demo.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @className: FlowBean
 * @description:
 * phone_data.txt 文件数据描述如下
 *    电话号码       IP地址            访问地址     上行流量     下行流量   状态码
 * 13736230513	192.196.100.1	www.atguigu.com	   2481	     24681	   200
 *
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/7
 */
public class FlowBean implements Writable {

    private Long upFlow;

    private Long downFlow;

    private Long sumFlow;

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    // 不考虑溢出情况
    public void setSumFlow(){
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return  upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }
}
