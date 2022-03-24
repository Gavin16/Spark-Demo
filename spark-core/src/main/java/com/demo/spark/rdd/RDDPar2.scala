package com.demo.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDPar2 {

    def main(args: Array[String]): Unit = {
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(config)

        /*
         测试textFile 读取数据后分区计算方式
             注意: textFile读取文件有以下几个特点
                   1) 数据是整行整行的读取
                   2) 数据已偏移量为下标进行读取
                   3) 不会重复读取相同的偏移量

         words.txt 中数据以如下形式存储

         123456\n
         7\n
         8\n
         9\n
         0

         macos 中换行符占一个字节,因此以上数据共计 14 字节
         textFile方式加载文件时,默认分区数为2(当字节数/2 余数大于10%才使用第三个分区)
         那么计算得到每个分区字节数为  14 / 2 = 7 可以整除。因此只会使用两个分区

         那么对于这两个分区，各个分区读取到的偏移量按如下方式划分
         分区1: 读取的偏移量范围为: [0,7]
         分区2: 读取的偏移量范围为: [7,14]
         每个范围都包含了右边区间, 因此分区1读到了第二行的 7 第二行的7会作为一整行被读取
         剩下则是 [7,14] 范围内所有的字节(以行为单位)


         ！！！ 若数据源为多个文件，


         */
        val rdd: RDD[String] = sc.textFile("datas/words.txt")

        rdd.saveAsTextFile("output")
        sc.stop()
    }

}
