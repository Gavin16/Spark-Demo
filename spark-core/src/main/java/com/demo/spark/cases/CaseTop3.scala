package com.demo.spark.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 实战案例: 统计每个省份每个广告被点击数量排行的 top3
 *
 * 统计agent.log 中每个省份每个广告被点击数量排行的 top3
 * 其中 agent.log 文件中每行包含如下信息
 * 时间戳,省份,城市,用户，广告
 *
 */
object CaseTop3 {

    def main(args: Array[String]): Unit = {
        // 实现案例需求的基本思路是: 缺什么补什么,多什么,删什么
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top3")
        val sc = new SparkContext(sparkConf)

        // 1.获取数据
        val dataRDD: RDD[String] = sc.textFile("datas/agent.log")
        // 2.数据做结构转换
        // 时间戳,省份,城市,用户，广告  =>  （(省份,广告),1)
        val mapRDD: RDD[((String, String), Int)] = dataRDD.map(line => {
            val datas: Array[String] = line.split(" ")
            ((datas(1), datas(4)), 1)
        })
        // 3.转换后的数据做分组聚合
        // （(省份,广告),1) =>  （(省份,广告),sum)
        val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

        // 4.将聚合的结果进行结构的转换
        // （(省份,广告),sum) => （省份,(广告,sum))
        val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
            case ((pro, ad), cnt) => (pro, (ad, cnt))
        }

        // 5.将转换后的数据根据省份进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()


        // 6.将分组后的数据进行组内排序,根据出现数量降序排序，取前三名
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse) take (3)
            }
        )

        // 7.输出结果
        resultRDD.collect().foreach(println)

        sc.stop()
    }
}
