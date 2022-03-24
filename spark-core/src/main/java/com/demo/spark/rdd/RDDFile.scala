package com.demo.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从文件中创建RDD
 *
 */
object RDDFile {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 通过 textFile 读取到的是文件内容
        // path路径默认以当期那环境的根路径为准,
        // 可以写绝对路径，也可以写成相对路径
        val rdd: RDD[String] = sc.textFile(path = "datas/1.txt")
        // 可以指定具体文件也可以指定具体目录
        val rdd2: RDD[String] = sc.textFile(path = "datas")
        // 还可以使用通配符 或者 分布式存储系统路径(HDFS) 等


        rdd.collect().foreach(println)
        rdd2.collect().foreach(println)

        sc.stop()

        readFromFile()
    }

    def readFromFile():Unit ={
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // wholeTextFiles 以文件为单位读取数据
        val rdd: RDD[(String, String)] = sc.wholeTextFiles(path = "datas")
        // 读取内容为元组形式, 第一个元素为文件名, 之后所有元素为文件内容
        rdd.collect().foreach(println)

        sc.stop()
    }
}
