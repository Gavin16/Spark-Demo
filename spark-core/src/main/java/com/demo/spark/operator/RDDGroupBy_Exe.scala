package com.demo.spark.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import java.util.Date

/**
 *
 * 从服务器日志数据apache.log 中获取每个时间段访问量
 *
 */
object RDDGroupBy_Exe {


    def main(args: Array[String]): Unit = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(config)

        val rdd: RDD[String] = sc.textFile("datas/apache.log")

        val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
            line => {
                val datas: Array[String] = line.split(" ")
                val time = datas(3)
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date: Date = sdf.parse(time)
                val format = new SimpleDateFormat("HH")
                val hour: String = format.format(date)
                (hour, 1)
            }
        ).groupBy(_._1)

        timeRDD.map {
            case (hour, tuples) => {
                (hour, tuples.size)
            }
        }.collect().foreach(println)


        sc.stop()


    }

}
