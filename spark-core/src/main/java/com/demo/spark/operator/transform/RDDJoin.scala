package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * join 操作API注释:
 * join操作返回两个RDD中所有key相互匹配的元素对，当(k,v1) 在一个rdd, 而(k,v2) 在另一个rdd 中时
 * 每对元素都将以 (k,(v1,v2)) tuple 的形式返回。 指定的分区也将转换到输出的分区.
 *
 */
object RDDJoin {


    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // join操作类似数据库表关联操作,但是返回结果都是以元素形式封装
        // (1) 若其中一个rdd 中有些key在另一个rdd中找不到时，则忽略该key
        // (2) 若其中一个rdd的key出现多次,则可以出现多次匹配(产生笛卡尔积)
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("f", 7)))
        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("c", 5), ("b", 4), ("d", 6), ("a", 2)))

        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        //  leftOuterJoin 以左边数据为主, 即使在右边RDD匹配不到对应的key仍然显示
        //  rightOuterJoin 类似
        rdd1.leftOuterJoin(rdd2).collect().foreach(println)
        rdd1.rightOuterJoin(rdd2).collect().foreach(println)


        joinRDD.collect().foreach(println)
        sc.stop()
    }
}
