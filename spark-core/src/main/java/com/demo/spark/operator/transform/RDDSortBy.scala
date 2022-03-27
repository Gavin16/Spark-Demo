package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy 排序
 */
object RDDSortBy {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 4), ("2", 3)), 2)

        val sortedRdd: RDD[(String, Int)] = rdd.sortBy(t => t._2)
        sortedRdd.collect().foreach(println)

        sc.stop()

    }
}
