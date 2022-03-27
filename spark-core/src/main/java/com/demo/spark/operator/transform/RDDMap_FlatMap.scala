package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 将整体拆分为一个一个的个体进行处理
 */
object RDDMap_FlatMap {

    def main(args: Array[String]): Unit = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(config)

        // list 中包含多种数据类型
        val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 5, List(3, 4)))

        // 使用模式匹配根据情况进行数据类型的转化
        val value: RDD[Any] = rdd.flatMap(
            list => {
                list match {
                    case list: List[_] => list
                    case data => List(data)
                }
            }
        )

        value.collect().foreach(println)

        val value1: RDD[Array[Any]] = value.glom()
        value1.collect().foreach(data => print(data.mkString(",")))

        sc.stop()

    }
}
