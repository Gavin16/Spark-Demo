package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDReduceByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("c", 2), ("a", 2), ("c", 1)))
        // 对相同的key 的value进行两两聚合
        // reduceByKey 中若key只有一个，则该key 不会参与运算
        //
        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
            println(s"x=${x} , y =${y}")
            x + y
        })

        reduceRDD.collect().foreach(println)


        sc.stop()
    }
}
