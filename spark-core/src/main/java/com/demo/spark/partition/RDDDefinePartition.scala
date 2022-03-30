package com.demo.spark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 自定义RDD 分区器
 *
 */
object RDDDefinePartition {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Partition")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, String)] = sc.makeRDD(List(
            ("NBA", "shkdfhsk"),
            ("CBA", "234hdfio"),
            ("NBA", "sjgfosjo2"),
            ("WBA", "9823dhc")
        ))

        val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartition())

        partRDD.saveAsTextFile("output")


        sc.stop()
    }

    // 自定义分区器需要继承 Partition
    // 指定分区数量 numPartitions
    // getPartition 通过k-v 数据类型中的key 返回RDD的分区号
    class MyPartition extends Partitioner{

        override def numPartitions: Int = 3

        // 根据数据的key值返回数据所在的分区索引(从 0 开始)
        override def getPartition(key: Any): Int = {
            key match {
                case "NBA" => 0
                case "CBA" => 1
                case "WBA" => 2
            }
        }
    }
}
