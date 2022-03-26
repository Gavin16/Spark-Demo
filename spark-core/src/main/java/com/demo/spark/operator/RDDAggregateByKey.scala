package com.demo.spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过key进行聚合
 */
object RDDAggregateByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)),
            2)
        val aggreRDD = rdd.aggregateByKey(5)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )
        aggreRDD.collect().foreach(println)
        sc.stop()
    }
}
