package com.demo.spark.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDExecutor {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd : RDD[Int] = sc.makeRDD(List[Int](1,2,3,4))

        val user = new User()

        rdd.foreach(num =>{
            println("age = " + (num + user.age))
        })
        sc.stop()
    }

    class User extends Serializable {
        var age:Int = 30;
    }
}
