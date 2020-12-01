package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.DauHandler
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.消费Kafka TOPIC_START主题数据,创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP_TOPIC, ssc)

    //4.将每行数据转换为样例类对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH") //Driver
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => { //Executor
      //a.取出Value数据
      val value: String = record.value()
      //b.转换为样例类对象
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //c.给时间字段重新赋值
      val ts: Long = startUpLog.ts
      val dateStr: String = sdf.format(new Date(ts))
      val dateArr: Array[String] = dateStr.split(" ")
      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1)
      //d.将结果返回
      startUpLog
    })

    //原始数据个数打印
    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    //5.利用Redis做跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)

    //跨批次去重之后个数打印
    filterByRedisDStream.cache()
    filterByRedisDStream.count().print()

    //6.使用Mid作为Key做同批次去重
    val filterByMid: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream)

    //同批次去重之后数据个数打印
    filterByMid.cache()
    filterByMid.count().print()

    //7.将两次去重之后的结果中的Mid写入Redis,给当天后置批次去重使用
    DauHandler.saveMidToRedis(filterByMid)

    //8.将两次去重之后的结果写入HBase(Phoenix)

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
