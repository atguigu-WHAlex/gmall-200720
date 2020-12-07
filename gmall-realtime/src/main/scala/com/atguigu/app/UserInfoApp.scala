package com.atguigu.app

import com.atguigu.constants.GmallConstant
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserInfoApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka USER_INFO主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO_TOPIC, ssc)

    //打印测试
    kafkaDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        iter.foreach(record => {
          println(record.value())
        })
      })
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
