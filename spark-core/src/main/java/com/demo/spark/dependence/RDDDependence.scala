package com.demo.spark.dependence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 依赖关系
 *
 * RDD计算过程保存了当前位置的所有计算过程: rdd.toDebugString 可以看到
 *
 */
object RDDDependence {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShowDependence")
        val sc = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("datas/words.txt")
        println(lines.toDebugString)
        println("*********************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        println("*********************")
        val wordMap: RDD[(String, Int)] = words.map(word => (word, 1))
        println(wordMap.toDebugString)
        println("*********************")
        val wordSum: RDD[(String, Int)] = wordMap.reduceByKey(_ + _)
        println(wordSum.toDebugString)
        println("*********************")
        val tuples: Array[(String, Int)] = wordSum.collect()
        tuples.foreach(println)

        sc.stop()
    }

}

