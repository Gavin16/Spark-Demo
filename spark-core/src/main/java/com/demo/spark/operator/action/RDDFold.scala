package com.demo.spark.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDFold {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List.range(1, 5),2)
        // 当 aggregate 中分区内与分区间的计算规则相同时，可以使用fold来替换
        // 这样fold第二个函数传参就只需要指定一个计算规则就行
        val result: Int = rdd.fold(10)(_ + _)
        println(result)

        sc.stop()

    }

}
