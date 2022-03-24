package com.demo.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 的并行度
 *
 * RDD并行度 和 分区有关系
 *
 */
object RDDPar {


    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//        sparkConf.set("spark.default.parallelism", "4")
        val sc = new SparkContext(sparkConf)

//        val value: RDD[String] = sc.textFile("datas/1.txt")
        val value: RDD[String] = sc.textFile("datas/1.txt", 2)
//        val rdd: RDD[Int] = sc.makeRDD(
//            List(1, 2, 3, 4,5,6,7,8,9), numSlices = 5
//        )

        // 将处理的数据保存成分区文件
        value.saveAsTextFile("output")

        sc.stop()
    }
}
