package com.demo.spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sample算子  从指定数据集中抽取数据
 * 用于数据采样后做轻量级分析
 */
object RDDSample {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6,7,8,9,10))
        // withReplacement  抽取后是否放回
        // fraction         每条数据可能包抽取的概率: 若withReplacement为true, 则表示可能被抽取的次数
        // seed             随机数算法种子: 默认使用当前系统时间
        val value: RDD[Int] = rdd.sample(false, 0.3)
        value.collect().foreach(println)

        sc.stop()
    }
}
