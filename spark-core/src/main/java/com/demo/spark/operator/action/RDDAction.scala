package com.demo.spark.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDAction {

    /**
     * RDD 行动算子
     * 行动算子包块如下几个:
     * collect(): 将不同分区的数据按照分区顺序采集到driver端内存中,形成数组
     * reduce(): 汇总得到结果
     * count(): 统计rdd中元素个数
     * take(): 取前N个元素
     */
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List.range(1, 6))

        // reduce 作为行动算子 会触发作业的执行
        val sum : Int = rdd.reduce(_ + _)
        println(sum)

        // 将不同分区的数据按照分区顺序采集到driver端内存中,形成数组
        val array: Array[Int] = rdd.collect()
        println(array.mkString(","))

        // 统计rdd中元素个数
        val count: Long = rdd.count()
        println(count)

        // 选取前N个元素
        val take: Array[Int] = rdd.take(3)
        println(take.mkString(","))

        // 排序后选取前N个元素
        val orderedTake: Array[Int] = rdd.takeOrdered(3)(Ordering.Int.reverse)
        println(orderedTake.mkString(","))

        sc.stop()
    }
}
