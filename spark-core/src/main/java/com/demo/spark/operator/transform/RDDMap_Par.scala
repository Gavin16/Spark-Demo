package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDMap_Par {


    // RDD 方法并行计算
    def main(args: Array[String]): Unit = {
        //        simpleMap
        mapPartitions
    }


    def simpleMap() = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(config)
        val value: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        val mapRDD: RDD[Int] = value.map(num => {
            println("num = " + num)
            num
        })
        val mapRDD1: RDD[Int] = mapRDD.map(num => {
            println(">>> " + num)
            num
        })
        mapRDD1.collect()
        sc.stop()
    }

    // 一次性的将一个分区所有的数据做映射,效率较 map 操作高
    def mapPartitions() = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(config)
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // 可以以分区为单位进行转换操作
        // 会将整个分区数据加载到内存中进行引用(存在对象引用), 当内存较小,数据量较大时 会出现内存溢出
        val mapPar: RDD[Int] = rdd.mapPartitions(iter => {
            println("=====")
            iter.map(_ * 2)
        })

        mapPar.collect().foreach(println)

        sc.stop()
    }
}
