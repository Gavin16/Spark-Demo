package com.demo.spark.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDObjectReuse {

    /**
     * 以下代码从打印结果来看
     * flatMap 执行了两次，因为复用了 mapRDD 执行了 reduceByKey 和 groupByKey 两个操作
     * 当再次执行groupByKey操作时，由于mapRDD中不保存数据，那么这时复用mapRDD对象进行操作就需要重新从数据的来源处读取
     * 因此 数据从来源到mapRDD的整个过程有需要重新走一次
     *
     * 所以flatMap 以及 map操作中的打印出现了多次
     */
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(line => {
            println("@@@@@@@@@@@@@@@@@")
            val strings: Array[String] = line.split(" ")
            strings
        })
        val mapRDD: RDD[(String, Int)] = words.map(str =>{
            println("&&&&&&&&&&&&&&&&")
            (str, 1)
        })

        val wordCount: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        wordCount.collect().foreach(println)
        println("************************")
        val group: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        group.collect().foreach(println)

        sc.stop()
    }
}
