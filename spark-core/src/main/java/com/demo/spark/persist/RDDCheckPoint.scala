package com.demo.spark.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDCheckPoint {

    /**
     * RDD 设置检查点
     * 1) 检查点需要落盘存储
     * 2) 数据保存在检查点之后，即使任务执行完了，检查点路径中文件也不会被删除
     * 3) 一般保存路径都是分布式存储系统 HDFS
     *
     *
     * cache & persist & checkpoint 比较
     *
     * cache: 将数据临时存储在内存中进行数据重用
     *        会在血缘关系中添加新的依赖,一旦出现问题，可以重头读取数据
     * persist: 将数据临时存储在磁盘文件中进行数据重用
     *          涉及到磁盘IO,性能较低,但是数据安全
     *          如果作业执行完毕,临时保存的数据文件就会丢失
     * checkpoint: 将数据长久的保存在磁盘文件中进行数据重用
     *            涉及到磁盘IO,性能较低,但是数据安全
     *            为了保证数据安全,所以一般情况下会独立执行作业
     *            为了能提高效率,一般情况下,是需要和cache联合使用
     *            执行过程中,会切断血缘关系,重新建立新的血缘关系
     *            checkpoint 等同于改变数据源
     */
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparkConf)
        // 检查点保存路径
        sc.setCheckpointDir("cp")

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
        // 设置检查点,联合cache使用以提升效率
//        mapRDD.cache()
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)

        val wordCount: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        wordCount.collect().foreach(println)
        println("************************")
        val group: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        group.collect().foreach(println)

        println(mapRDD.toDebugString)

        sc.stop()

    }
}
