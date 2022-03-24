package com.demo.spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy 不会改变分区数量,但是分区中的数据会被打乱
 * 将这样的操作叫做 shuffle。
 * 一个组的数据只会在一个分区中,但是一个分区中可能有多个组。极端情况下所有数据
 * 可能都落在一个分区中。
 *
 */
object RDDGroupBy {

    def main(args: Array[String]): Unit = {

        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(config)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 3))
        def groupByFunc(num:Int)={
            num
        }

        val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupByFunc)

        groupByRDD.collect().foreach(println)

        val rdd1: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"))
        val rdd1Group: RDD[(Char, Iterable[String])] = rdd1.groupBy(_.charAt(0))
        rdd1Group.collect().foreach(println)
        rdd1Group.saveAsTextFile("output")

        sc.stop()
    }
}
