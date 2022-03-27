package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD filter 算子应用
 */
object RDDFilter {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val value: RDD[Int] = rdd.filter(num => num % 2 == 1)
        value.collect().foreach(println)
        sc.stop()

        // test
        filterTest()

    }

    /**
     * 日志文件 apache.log 中筛选出 "17/05/2015" 当天的日志
     * 并打印输出日志的访问IP 和 具体时间
     */
    def filterTest() = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("datas/apache.log")

        rdd.filter(
            line => {
                val datas: Array[String] = line.split(" ")
                val time: String = datas(3)
                time.startsWith("17/05/2015")
            }
        ).map(line => {
            val strings: Array[String] = line.split(" ")
            (strings(0), strings(3))
        }).collect().foreach(println)


        sc.stop()
    }

}
