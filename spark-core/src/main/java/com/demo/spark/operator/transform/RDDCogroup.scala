package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDCogroup {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        // cogroup = connect + group
        // cogroup 可以连接多个 RDD
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("f", 7)))
        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("c", 5), ("b", 4), ("d", 6), ("a", 2)))

        val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        value.collect().foreach(println)
        sc.stop()
    }
}
