package com.demo.mapreduce.writableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @className: FlowBean
 * @description:
 *
 * 使用FlowBean 作为key 需要实现 WritableComparable 接口
 * 通过对 key 中 sumFlow 进行倒序排序以实现需求
 *
 *
 * @version: 1.0
 * @author: minsky
 * @date: 2022/4/7
 */
public class FlowBean implements Writable, WritableComparable<FlowBean> {

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

    @Override
    public int compareTo(FlowBean o) {

        if(this.sumFlow > o.sumFlow){
            return -1;
        }else if(this.sumFlow < o.sumFlow){
            return 1;
        }else{
            if(this.upFlow > o.upFlow){
                return 1;
            }else if(this.upFlow < o.upFlow){
                return -1;
            }
            return 0;
        }
    }
}
