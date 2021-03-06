package com.atguigu.app

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{JdbcUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
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
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinResult.mapPartitions(iter => {

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

          //a.3 查询Detail数据
          val detailSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          detailSet.asScala.foreach(detailJson => {
            val orderDetail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
            details += new SaleDetail(orderInfo, orderDetail)
          })

        }
        else {

          //b.info数据为空
          //获取Detail数据
          val orderDetail: OrderDetail = detailOpt.get

          //查询Redis中是否存在对应的Info数据
          if (jedisClient.exists(infoRedisKey)) {

            //b.1 Redis中存在Info数据,查询出Info数据,集合写入集合
            val infoJson: String = jedisClient.get(infoRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])
            details += new SaleDetail(orderInfo, orderDetail)

          } else {

            //b.2 Redis中不存在Info数据,将自身写入Redis等待后续批次的Info数据
            val detailStr: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, detailStr)
            jedisClient.expire(detailRedisKey, 100)
          }

        }
      }

      //归还连接
      jedisClient.close()

      //最终返回值
      details.toIterator
    })

    //6.反查Redis,补充用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {

      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //b.遍历,查询Redis中用户信息并补充
      val result: Iterator[SaleDetail] = iter.map(saleDetail => {

        val userRedisKey = s"UserInfo:${saleDetail.user_id}"

        //查询Redis中是否存在该用户信息
        if (jedisClient.exists(userRedisKey)) {

          //Redis中存在,查询Redis,说明该用户有下单,即近期活跃过
          val userInfoStr: String = jedisClient.get(userRedisKey)
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)

          //重置过期时间
          jedisClient.expire(userRedisKey, 24 * 60 * 60)

          saleDetail
        } else {

          //Redis中不存在,查询MySQL
          //获取连接
          val connection: Connection = JdbcUtil.getConnection

          //查询数据
          val userInfoStr: String = JdbcUtil.getJsonDataFromMysql(connection,
            "select * from user_info where id = ?",
            Array(saleDetail.user_id))

          println(userInfoStr)

          //将数据写入Redis
          jedisClient.set(s"UserInfo:${saleDetail.user_id}", userInfoStr)
          jedisClient.expire(s"UserInfo:${saleDetail.user_id}", 24 * 60 * 60)

          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)

          connection.close()

          saleDetail
        }
      })

      //c.归还连接
      jedisClient.close()

      //d.返回结果数据
      result
    })

    //打印测试
    //    noUserSaleDetailDStream.print(100)
    //    saleDetailDStream.print(100)
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

    //7.将数据写入ES
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        val list: List[(String, SaleDetail)] = iter.map(saleDetail => (s"${saleDetail.order_id}_${saleDetail.order_detail_id}", saleDetail)).toList

        val indexName = s"${GmallConstant.GMALL_SALE_DETAIL_ES_PREFIX}_${sdf.format(new Date(System.currentTimeMillis()))}"

        MyEsUtil.insertBulk(indexName, list)

      })

    })

    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
