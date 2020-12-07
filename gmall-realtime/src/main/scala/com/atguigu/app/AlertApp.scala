package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka事件日志主题数据,创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_EVENT_TOPIC, ssc)

    //3.将每行数据转换为样例类对象,并补充时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(record => {

      //a.将value转换为样例类对象
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //b.补充时间字段
      val dateHourStr: String = sdf.format(new Date(eventLog.ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      //c.将对象返回
      (eventLog.mid, eventLog)

    })

    //4.开窗
    val windowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //5.按照Mid进行分组
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    //6.在组内,对数据进行筛选
    val boolToAlertInfo: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.map { case (mid, iter) =>

      //a.创建HashSet用于存放uid,做到去重
      val uids: util.HashSet[String] = new util.HashSet[String]()
      //b.创建HashSet用于存放ItemId
      val itemIds = new util.HashSet[String]()
      //c.创建List用于存放行为数据
      val events = new util.ArrayList[String]()

      //定义一个标志位,用于区分是否有浏览商品行为
      var noClick: Boolean = true

      //d.遍历迭代器,向三个集合中添加数据,注意条件
      breakable {
        iter.foreach(log => {
          //提取事件行为
          val evid: String = log.evid
          //将事件行为放入events
          events.add(evid)

          //判断当前行为是否为领券行为
          if ("coupon".equals(evid)) {
            uids.add(log.uid)
            itemIds.add(log.itemid)
          } else if ("clickItem".equals(evid)) {
            noClick = false
            break()
          }

        })
      }

      //e.根据uids判断是否需要产生预警日志
      //      if (uids.size() >= 3 && noClick) {
      //        //产生预警日志
      //        CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis())
      //      } else {
      //        null
      //      }
      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }
    val alertInfoDStream: DStream[CouponAlertInfo] = boolToAlertInfo.filter(_._1).map(_._2)

    //7.给定DocId
    val docIdToAlertInfoDStream: DStream[(String, CouponAlertInfo)] = alertInfoDStream.map(alertInfo => {
      (s"${alertInfo.mid}-${alertInfo.ts / 1000L / 60}", alertInfo)
    })

    //    docIdToAlertInfoDStream.print(100)

    //8.将数据写入ES
    docIdToAlertInfoDStream.foreachRDD(rdd => {
      println("计算开始了！！！")
      rdd.foreachPartition(iter => {
        val date: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        MyEsUtil.insertBulk(s"${GmallConstant.GMALL_ALERT_INFO_ES_PREFIX}_$date", iter.toList)
      })
    })

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
