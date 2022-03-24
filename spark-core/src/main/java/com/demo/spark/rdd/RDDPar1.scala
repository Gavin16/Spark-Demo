package com.demo.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 数据分区的分配
 *
 */
object RDDPar1 {


    def main(args: Array[String]): Unit = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(config)
        // spark 读取文件采用hadoop 的方式读取, 所以一行一行读取，和字节数没有关系
        // 1.数据以行位单位进行读取
        // 2.数据读取时 以偏移量为单位
        /*
            1@@  => 0,1,2
            2@@  => 3,4,5
            3    => 6
         */
        // 3.数据分区偏移量范围计算方式:
        // 分区0 => [0,3]  读取到1，2
        // 分区1 => [3,6]  读取到3
        // 分区2 => [6,7]  读取到空
        val value: RDD[String] = sc.textFile("datas/1.txt")
        value.saveAsTextFile("output")
        sc.stop()
    }
}
