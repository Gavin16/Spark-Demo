package com.demo.spark.accumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Spark Broadcast(广播变量): 分布式共享只读变量
 */
object SparkBroadcast {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Accumulator")
        val sc = new SparkContext(sparkConf)

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("b", 4), ("c", 5)))

        // 使用join方式以key形式聚合
//        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//        joinRDD.collect().foreach(println)

        // 使用广播变量保存共享数据
        val map: mutable.Map[String, Int] = mutable.Map(("a", 3), ("b", 4), ("c", 5))
        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

        // 广播变量的使用 broadcast.value
        rdd1.map{
            case (word,cnt) => {
                val cnt1: Int = bc.value.getOrElse(word, 0)
                (word, (cnt,cnt1))
            }
        }.collect().foreach(println)

        sc.stop()
    }
}
