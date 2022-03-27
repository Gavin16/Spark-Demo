package com.demo.spark.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDAggregate {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List.range(1, 5),2)
        // 行动算子 aggregate 与 aggregateByKey 的区别
        // aggregateByKey: 初始值只会参与分区内的计算
        // aggregate: 初始值会参与分区内计算，并且也会参与分区间的计算
        // 注: aggregate 在使用时,结果与分区数量有关!!!
        val result: Int = rdd.aggregate(10)(_ + _, _ + _)
        println(result)

        sc.stop()
    }
}
