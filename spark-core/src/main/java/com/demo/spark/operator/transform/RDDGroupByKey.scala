package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDGroupByKey {

    def main(args: Array[String]): Unit = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(config)

        // groupByKey 将数据源中相同key的数据分在同一个组中，形成一个对偶元组
        // 元组中第一个元素就是 key
        // 元组中第二个元素就是 相同key的value的集合
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 1)))
        val groupedRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
        groupedRDD.collect().foreach(println)
        // groupBy 通过分组时需要指定以什么作为分组，那么在分组时，组内的数据是分组前的数据
        // groupByKey 由于在分组时知道了key 是哪一个，因此分组后组内数据就是value的集合
        val value: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
        value.collect().foreach(println)

        sc.stop()
    }
}
