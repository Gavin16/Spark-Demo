package com.demo.spark.dependence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDDependency {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("datas/words.txt")

        // rdd dependencies查看相邻RDD 之间的依赖关系
        // OneToOne依赖: 新的RDD中一个分区的数据依赖前面相邻的一个分区的数据
        // Shuffle 依赖: 新的RDD中一个分区的数据依赖前面多个分区的数据(做了shuffle打乱重组)
        // 当出现Shuffle依赖时，RDD中将单独生成一个stage
        // 计算过程中 总stage数 = shuffle 操作数(宽依赖数) + 1 (默认都存在一个ResultStage)
        println(rdd.toDebugString)
        println("*********************")
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        println(words.dependencies)
        println("*********************")
        val wordMap: RDD[(String, Int)] = words.map(word => (word, 1))
        println(wordMap.dependencies)
        println("*********************")
        val wordSum: RDD[(String, Int)] = wordMap.reduceByKey(_ + _)
        println(wordSum.dependencies)
        println("*********************")
        val tuples: Array[(String, Int)] = wordSum.collect()
        tuples.foreach(println)



        sc.stop()
    }
}
