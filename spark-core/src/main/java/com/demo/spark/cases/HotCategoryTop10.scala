package com.demo.spark.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 电商热门商品 top10
 *
 */
object HotCategoryTop10 {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GoodsTop10")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("user_visit_action.csv")

        sc.stop()
    }

}

