package com.demo.spark.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDLoadFile {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("FileLoad")
        val sc = new SparkContext(sparkConf)

        // 保存RDD中数据到本地
//        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
//            ("a", 1), ("b", 1), ("c", 2)
//        ))
//        rdd.saveAsTextFile("output1")
//        rdd.saveAsObjectFile("output2")
//        rdd.saveAsSequenceFile("output3")

        // 读取本地RDD数据: 注意数据类型
        val rdd1: RDD[String] = sc.textFile("output1")
        val rdd2: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
        val rdd3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output3")

        println(rdd1.collect().mkString(","))
        println(rdd2.collect().mkString(","))
        println(rdd3.collect().mkString(","))

        sc.stop()
    }
}
