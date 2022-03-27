package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 *
 */
object RDDPartitionBy {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDPartitionBy")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val kvRDD: RDD[(Int, Int)] = rdd.map((_, 1))
        // partitionBy 不是RDD 的方法,而是PairRDDFunctions 中的方法
        // RDD中定义了隐式转换方法 rddToPairRDDFunctions  将元组类型的RDD包装成了 PairRDDFunctions 对象
        // 编译器在程序编译出现错误的时候,会尝试在作用域范围内查询转换规则,看是否可以将对象转换为特定的类型之后
        // 让它编译通过。因此隐私转换又叫做 二次编译
        // partitionBy 需要指定分区器作为传参
        // 常用的分区器有如下几个:
        // HashPartition, RangePartition, PythonPartition
        val newRDD: RDD[(Int, Int)] = kvRDD.partitionBy(new HashPartitioner(2))
        newRDD.saveAsTextFile("output")

        sc.stop()
    }
}
