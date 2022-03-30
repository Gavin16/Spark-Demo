package com.demo.spark.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark_ACC_WordCount {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("DefineAccumulator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(List("Hello", "Scala", "Hello", "Spark"))

        val acc = new MyAccumulator()
        sc.register(acc,"WordCountAcc")

        rdd.foreach(word => {
            acc.add(word)
        })
        println(acc.value)

        sc.stop()
    }

    /**
     * 自定义累加器需要继承 AccumulatorV2 抽象类
     * abstract class AccumulatorV2[IN, OUT] extends Serializable
     *
     * IN:累加器输入的数据类型 对于单个单词类型为 String
     * OUT: 累加器返回的数据类型 返回结果是map类型，因此结果为 mutable.Map[String,Long]
     *
     * 重写 AccumulatorV2 中声明的抽象方法
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String,Long]]{

        private val wcMap = mutable.Map[String, Long]()

        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.Map[String,Long]] = {
            new MyAccumulator()
        }

        override def reset(): Unit = {
            wcMap.clear()
        }

        override def add(word: String): Unit = {
            val newCnt = wcMap.getOrElse(word, 0L) + 1
            wcMap.update(word,newCnt)
        }

        // spark driver中合并executor各累积器结果  => 合并结果保存在wcMap 中
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1 = this.wcMap
            val map2 = other.value

            map2.foreach({
                case (word,count) =>{
                    val newCnt: Long = map1.getOrElse(word, 0L) + count
                    map1.update(word, newCnt)
                }
            })
        }

        override def value: mutable.Map[String, Long] = {
            wcMap
        }
    }
}
