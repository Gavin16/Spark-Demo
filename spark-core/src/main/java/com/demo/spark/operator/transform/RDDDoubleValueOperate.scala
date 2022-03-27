package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 双value 操作: 交集, 并集 & 拉链
 *
 */
object RDDDoubleValueOperate {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

        // 求rdd1 和 rdd2的交集
        val interRdd: RDD[Int] = rdd1.intersection(rdd2)
        println(interRdd.collect().mkString(","))

        // rdd1 并上 rdd2
        val unionRdd: RDD[Int] = rdd1.union(rdd2)
        println(unionRdd.collect().mkString(","))

        // rdd1 减去 rdd2
        val subRdd: RDD[Int] = rdd1.subtract(rdd2)
        println(subRdd.collect().mkString(","))

        // rdd1 与 rdd2 按位置逐个拉链
        // 拉链要求两个rdd 的分区数量一致，且各分区中数据量也一样(数据类型可以不同)
        val zipRdd: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println(zipRdd.collect().mkString(","))

        sc.stop()


    }
}
