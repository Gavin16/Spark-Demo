package com.demo.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * word count 实现方式汇总
 *
 */
object WCSummary {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        sc.stop()
    }

    /**
     * groupBy 实现
     * 先分组,再统计每组数量
     */
    def wordCount1(sc: SparkContext)={
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(line => {line.split(" ")})
        val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
        val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
        wordCount.collect().foreach(println)
    }

    /**
     * groupByKey 实现
     * 将 word 映射为 (word,1) 元组后再进行分组
     */
    def wordCount2(sc: SparkContext) = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordMap: RDD[(String, Int)] = words.map((_, 1))
        val group: RDD[(String, Iterable[Int])] = wordMap.groupByKey()
        val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
        wordCount.collect().foreach(println)
    }


    /**
     * reduceByKey
     * word 映射为 (word,1) 后，可以根据key 直接进行reduce 操作
     *
     */
    def wordCount3(sc: SparkContext) = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordMap: RDD[(String, Int)] = words.map((_, 1))
        val wordCount: RDD[(String, Int)] = wordMap.reduceByKey(_ + _)
        wordCount.collect().foreach(println)
    }

    /**
     * aggregateByKey
     * word 映射为 (word,1) 元组后,由于key是已知的
     * 通过aggregateByKey 操作可以对key进行聚合
     */
    def wordCount4(sc:SparkContext)={
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))

        val wordMap: RDD[(String, Int)] = words.map((_, 1))
        val wordCount: RDD[(String, Int)] = wordMap.aggregateByKey(0)(_ + _, _ + _)
        wordCount.collect().foreach(println)
    }

    /**
     * foldByKey
     * 对于aggregateByKey 当分区间和分区内的操作相同，可以使用fold操作替代
     */
    def wordCount5(sc:SparkContext)={
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))

        val wordMap: RDD[(String, Int)] = words.map((_, 1))
        val wordCount: RDD[(String, Int)] = wordMap.foldByKey(0)(_ + _)
        wordCount.collect().foreach(println)
    }

    /**
     * combineByKey
     * combineByKey 是 aggregateByKey 等聚合操作的底层实现
     * 相对更加一般化
     */
    def wordCount6(sc:SparkContext)={
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordMap: RDD[(String, Int)] = words.map((_, 1))

        val combineByKey: RDD[(String, Int)] = wordMap.combineByKey(
            v => v,
            (x: Int, y) => x + y,
            (x: Int, y: Int) => x + y
        )
        combineByKey.collect().foreach(println)
    }

    /**
     * countByKey
     * 对于(word,1) 的数据，直接统计每个key的数量就是在计数了
     *
     */
    def wordCount7(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordMap = words.map((_,1))

        val wordCount: collection.Map[String, Long] = wordMap.countByKey()
        wordCount.foreach(println)
    }

    /**
     * countByValue
     * 对于 word 数据, countByValue就是直接统计各个word的个数

     */
    def wordCount8(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordCount: collection.Map[String, Long] = words.countByValue()
        wordCount.foreach(println)
    }
}

