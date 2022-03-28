package com.demo.spark.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDPersist {


    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(line => {
            println("@@@@@@@@@@@@@@@@@")
            val strings: Array[String] = line.split(" ")
            strings
        })
        val mapRDD: RDD[(String, Int)] = words.map(str =>{
            println("&&&&&&&&&&&&&&&&")
            (str, 1)
        })
        // 持久化算子缓存时机: 行动算子执行时(这时才有数据)
        // RDD操作不一定是为了重用，也可能是计算时间较长或者较重要的场合
        // cache 默认持久化操作只能将数据保存到内存中
        mapRDD.cache()
        // persist 操作可以通过传参 StorageLevel指定存储级别
//        mapRDD.persist()

        val wordCount: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        wordCount.collect().foreach(println)
        println("************************")
        val group: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        group.collect().foreach(println)

        sc.stop()
    }
}
