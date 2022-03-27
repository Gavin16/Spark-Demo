package com.demo.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过key进行聚合
 *
 * aggregateByKey API解释：
 * 使用指定的连接函数和一个中立的"零值" 聚合RDD 各个key的 value，这个函数可以返回一个不同的结果类型 U
 * 而不是RDD中原有的类型 V. 因此，我们需要一个合并 V 到 U 的一个操作  以及一个操作用来合并两个 U类型的值。
 * 在scala中 可以做到， 前一个操作用来在分区内合并值, 后一个操作用来用来在不同分区间合并值。为了避免内存分配，
 * 这两个函数是被允许去修改和返回第一个参数的，而不是非得要创建一个新的U.
 *
 */
object RDDAggregateByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)),
            2)
        val aggreRDD = rdd.aggregateByKey(0)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )
        aggreRDD.collect().foreach(println)

        // 获取相同 key 的数据的平均值 => (b,4) (a,3)
        // 要计算如上部分功能, 实际写法如下
        // 这里 aggregateByKey 操作要实现的功能应该是输入 (K,V) 输出 (K,U) 其中 U = (sum, count)
        // aggregateByKey 第一个参数传的是 聚合需要获取到的值
        // 由于计算平均值需要用到 sum 和 count ,因此实际上还需要引入次数的计数
        // 因此第一个参数的 (0,0) 中第一个0可以代表是sum的初始值, 第二个0代表count的初始值
        // aggregateByKey 第二个参数括号中参数分两部分(参考 aggregateByKey 源码注释)：
        // 第一部分指的分区内的计算逻辑:
        // 分区内的计算逻辑中的 tuple 对应(t,v) 中t代表第一个参数(sum,count) 的数据类型,而第二个参数则为RDD做 aggregateByKey 前的value
        // 第二部分指的分区间的计算逻辑:
        // 操作的对象变成了(U,U)，因此这里需要将(U1,U2) U1,U2 的sum 和 count 部分分别相加并发返回
        // 最终 aggregateByKey 得到的将是 (key,(sum,count)) 类型的RDD
        val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
            (t, v) => {
                (t._1 + v, t._2 + 1)
            },
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )
        println(newRDD.collect().mkString(","))
        newRDD.mapValues {
            case (num, cnt) => {
                num / cnt
            }
        }.collect().foreach(println)

        sc.stop()
    }
}
