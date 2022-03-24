package com.demo.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建RDD
 *
 */
object RDDMemory {

    def main(args: Array[String]): Unit = {
        // 环境准备
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 创建RDD -- 从内存中创建RDD,将内存中集合的数据作为处理的数据源
        val seq = Seq[Int](elems = 1,2,3,4)
        val rdd: RDD[Int] = sc.makeRDD(seq)

        rdd.collect().foreach(println)

        // 关闭环境
        sc.stop()

    }

}
