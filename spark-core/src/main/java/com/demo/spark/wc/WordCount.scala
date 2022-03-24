package com.demo.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 单词统计
 *
 *
 */
object WordCount {

    def main(args: Array[String]): Unit = {
//        count_1()
//        count_2()
        count_3()
    }



    def count_3(): Unit={
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val context = new SparkContext(sparkConf)

        val lines: RDD[String] = context.textFile("datas")
        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        wordToCount.foreach(println)

        context.stop()
    }


    def count_2(): Unit={
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val context = new SparkContext(sparkConf)

        val lines: RDD[String] = context.textFile("datas")
        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)

        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) => {
                list.reduce((t1, t2) => {
                    (t1._1, t1._2 + t2._2)
                })
            }
        }
        wordToCount.foreach(println)

        context.stop()
    }


    def count_1(): Unit={
        // 建立与spark框架的连接 + 创建上下文
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val context = new SparkContext(sparkConf)

        /** spark业务操作 */
        // 读取文件 并做扁平化操作
        val lines: RDD[String] = context.textFile("datas")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) => {
                (word, list.size)
            }
        }

        val tuples: Array[(String, Int)] = wordToCount.collect()
        tuples.foreach(println)
        // 关闭连接
        context.stop()
    }
}
