package com.atguigu.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateTest {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aaa")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("12", "234", "345", "4567"), 2)

    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))
    println(rdd.aggregate("")((x, y) => Math.min(x.length, y.length).toString, (a, b) => a + b))

    sc.stop()


  }


}
