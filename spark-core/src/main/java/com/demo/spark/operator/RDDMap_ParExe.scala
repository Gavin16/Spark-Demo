package com.demo.spark.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDMap_ParExe {


    // 练习: 映射时做过滤处理
    def main(args: Array[String]): Unit = {

        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(config)
//        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
        val rdd: RDD[String] = sc.textFile("datas/characters.txt")

//        val mappedRDD: RDD[Int] = rdd.mapPartitions(
//            iter => {
//                println(">>>> ")
//                List(iter.max).iterator
//            }
//        )

        // mapPartitionsWithIndex 指定分区进行处理
        val filterdRDD: RDD[String] = rdd.mapPartitionsWithIndex((index, iter) => {
            if (index == 1) {
                iter
            } else {
                Nil.iterator
            }
        })

        val mapWithPar: RDD[(Int, String)] = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                iter.map(num => {
                    (index, num)
                })
            }
        )


//        mappedRDD.collect().foreach(println)
        filterdRDD.collect().foreach(println)
        mapWithPar.collect().foreach(println)
        sc.stop()
    }
}
