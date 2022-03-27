package com.demo.spark.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDForeach {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List.range(1, 5), 2)

        // 两个foreach 的区别:
        // rdd.collect().foreach(println) 是在driver端内存几个的循环遍历
        // rdd.foreach(println) 是在Executor端内存数据打印
        // 算子:
        // RDD中的操作(Operator) 之所以叫做算子,主要是因为如下原因:
        // RDD的方法和scala集合对象的方法不一样
        // 集合对象的方法都是在同一个节点的内存中完成
        // 而RDD的方法需要将计算逻辑发送到 Executor 端(分布式节点) 执行
        // 为了区分不同的处理效果, 因此将RDD的方法称为算子
        // RDD 方法外部的操作都是在Driver端的main方法中执行，而方法内部的逻辑代码是在Executor端执行的

        rdd.collect().foreach(println)
        rdd.foreach(println)


        sc.stop()
    }
}
