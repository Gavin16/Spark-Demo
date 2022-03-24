package com.demo.spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 考虑RDD 中的如下算子(操作)
 *
 * 1. map/mapPartitions/mapPartitionsWithIndex/flatMap
 * 2. groupBy
 *
 *
 */
object RDDMap {

    def main(args: Array[String]): Unit = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(config)

        // 算子 -- map
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 转换函数
//        def mapFunc(num: Int):Int={
//            num * 2
//        }

        // 转换lambda 表达式写法
//        val mapRes: RDD[Int] = rdd.map((e: Int) => 2 * e)
//        val mapRes: RDD[Int] = rdd.map(e => 2 * e)
        // 参数在逻辑中只出现一次, 且是顺序出现,可用下划线"_" 代替元素

        // map 将处理的数据逐条进行映射转换,这里的转换可以是类型的转换,也可以是值的转换
        val mapRes: RDD[Int] = rdd.map(_ * 2)
        mapRes.collect().foreach(println)
        sc.stop()

        mapRowToUrl()
    }

    /**
     * 获取日志中的URL
     * @param rdd
     */
    def mapRowToUrl()={
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(config)

        val source: RDD[String] = sc.textFile("datas/apache.log")

        val urls: RDD[String] = source.map(line => {
            val datas: Array[String] = line.split(" ")
            datas(6)
        })

        urls.collect().foreach(println)

        sc.stop()
    }

}
