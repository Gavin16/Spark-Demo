package com.demo.spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * distinct 数据去重处理
 */
object RDDDistinct {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 1, 2, 4))


        rdd.distinct().collect().foreach(println)

        sc.stop()
    }
}
