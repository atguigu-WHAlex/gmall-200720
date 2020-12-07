package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka order_info和order_detail主题数据创建流
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO_TOPIC, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL_TOPIC, ssc)

    //3.将两个流的数据转换为样例类对象,并转换为元组类型
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {

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
      (orderInfo.id, orderInfo)
    })
    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (detail.order_id, detail)
    })

    //4.做普通的双流JOIN:丢数据
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //4.FullOuterJoin
    val fullJoinResult: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //5.使用分区操作代替单条数据操作
    fullJoinResult.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放关联上的结果
      val details = new ListBuffer[SaleDetail]

      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //遍历迭代器,对单条数据做处理
      iter.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //创建RedisKey
        val infoRedisKey = s"OrderInfo:$orderId"
        val detailRedisKey = s"OrderDetail:$orderId"

        //判断infoOpt是否为空
        if (infoOpt.isDefined) {

          //a.info数据不为空
          val orderInfo: OrderInfo = infoOpt.get

          //a.1 判断detail数据是否为空
          if (detailOpt.isDefined) {
            //detail数据不为空
            val orderDetail: OrderDetail = detailOpt.get
            details += new SaleDetail(orderInfo, orderDetail)
          }

          //a.2 将自身写入Redis
          //将orderInfo对象转换为JSON串
          //val str: String = JSON.toJSONString(orderInfo) //编译报错
          val infoStr: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, infoStr)
          jedisClient.expire(infoRedisKey, 100)

        } else {

        }

      }

      //归还连接
      jedisClient.close()

      //最终返回值
      details.toIterator
    })


    //打印测试
    //    value.print()
    //    orderInfoKafkaDStream.foreachRDD(rdd => {
    //      rdd.foreachPartition(iter => {
    //        iter.foreach(record => {
    //          println(record.value())
    //        })
    //      })
    //    })
    //    orderDetailKafkaDStream.foreachRDD(rdd => {
    //      rdd.foreachPartition(iter => {
    //        iter.foreach(record => {
    //          println(record.value())
    //        })
    //      })
    //    })

    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
