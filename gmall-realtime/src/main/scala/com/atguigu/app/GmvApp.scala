package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //2.读取Kafka  OrderInfo主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO_TOPIC, ssc)

    //3.将每行数据转换为样例类对象(补充时间字段,对手机号进行脱敏处理)
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {

      //a.获取value信息
      val value: String = record.value()

      //b.将数据转换为样例类对象
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

      //c.对手机号进行脱敏
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = s"${tuple._1}*******"

      //d.补充时间字段
      val create_time: String = orderInfo.create_time //yyyy-MM-dd HH:mm:ss
      val dateTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)

      //e.返回结果
      orderInfo

    })

    //打印测试
    orderInfoDStream.cache()
    orderInfoDStream.print(100)

    //4.将数据写入Phoenix
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL200720_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
