package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * repartition 修改分区数量; 增大分区数
 *
 */
object RDDRepartition {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

        val value: RDD[Int] = rdd.coalesce(3, true)

        //        val value: RDD[Int] = rdd.repartition(3)
        //        value.saveAsTextFile("output")
        value.saveAsTextFile("output")

        sc.stop()
    }
}
