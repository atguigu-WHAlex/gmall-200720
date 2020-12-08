package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserInfoApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka USER_INFO主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO_TOPIC, ssc)

    //3.将数据(新增及变化数据)写入Redis
    kafkaDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.遍历写库
        iter.foreach(record => {

          //待写入数据
          val userInfoStr: String = record.value()

          //转换为样例类对象
          val info: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          jedisClient.set(s"UserInfo:${info.id}", userInfoStr)

          //防止冷数据在Redis中长期保存
          jedisClient.expire(s"UserInfo:${info.id}", 24 * 60 * 60)

        })

        //c.归还连接
        jedisClient.close()

      })

    })

    //打印测试
    //    kafkaDStream.foreachRDD(rdd => {
    //      rdd.foreachPartition(iter => {
    //        iter.foreach(record => {
    //          println(record.value())
    //        })
    //      })
    //    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
