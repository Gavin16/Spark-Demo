package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * coalesce 缩减/扩大分区数量
 *
 */
object RDDCoalesce {


    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
        // coalesce 默认不会将数据打乱重新组合
        // 可能导致数据不均衡, 解决办法: shuffle 参数指定为true
        // 注意: 若使用coalesce 扩大分区 shuffle 参数需要指定为true 否则不生效
        val newRdd: RDD[Int] = rdd.coalesce(2, true)
        newRdd.saveAsTextFile("output")

        sc.stop()
    }
}
