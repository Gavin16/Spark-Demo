package com.demo.spark.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 默认提供了简单数据聚合的累加器
 * 用于 executor 中执行累加后的结果返回
 *
 */
object SparkAccumulator {


    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Accumulator")
        val sc = new SparkContext(sparkConf)


        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val acc: LongAccumulator = sc.longAccumulator("Sum")

        rdd.foreach(num => acc.add(num))
        println(acc.value)
        // 累加器可能出现少加 或者 多加的情况
        // 少加: 如只在转换算子中使用累加器，后续没有调用行动算子
        // 多加: 多次调用行动算子
        // 为了避免出错，累加器一般放在行动算子中进行使用

        sc.stop()
    }
}
