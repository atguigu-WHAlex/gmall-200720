package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  /**
    * 使用Mid作为Key做同批次去重
    *
    * @param filterByRedisDStream 经过Redis跨批次去重之后的结果
    */
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.将数据转换为元组
    val midDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => ((log.mid, log.logDate), log))

    //2.按照key分组
    val midDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()

    //3.排序取时间最小的一条数据
    //    val midDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midDateToLogIterDStream.mapValues(iter => {
    //      iter.toList.sortWith(_.ts < _.ts).take(1)
    //    })
    //    //4.map
    //    val logListDStream: DStream[List[StartUpLog]] = midDateToLogListDStream.map(_._2)
    //    //5.压平
    //    val value: DStream[StartUpLog] = logListDStream.flatMap(list => list)

    val value1: DStream[StartUpLog] = midDateToLogIterDStream.flatMap { case ((mid, date), iter) =>
      iter.toList.sortWith(_.ts < _.ts).take(1)
    }

    //第一：Key是否需要保留,第二:是否需要压平
    //map：不需要Key,也不需要压平
    //mapValues:需要Key,不需要压平
    //flatMap:不需要Key,但是需要压平
    //flatMapValues:需要Key,同时需要压平

    //6.返回结果
    value1

  }


  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 利用Redis做跨批次去重
    *
    * @param startUpLogDStream 从kafka读取的原始数据
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一:使用filter算子
    //    val value1: DStream[StartUpLog] = startUpLogDStream.filter(log => {
    //      //a.获取Redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.查询Redis中是否存在mid
    //      val exist: lang.Boolean = jedisClient.sismember(s"DAU:${log.logDate}", log.mid)
    //      //c.归还连接
    //      jedisClient.close()
    //      //d.返回结果
    //      !exist
    //    })

    //方案二:使用分区操作代替单条数据操作
    //    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
    //      //a.获取连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.遍历iter,过滤数据
    //      val filterLogs: Iterator[StartUpLog] = iter.filter(log => {
    //        !jedisClient.sismember(s"DAU:${log.logDate}", log.mid)
    //      })
    //      //c.归还连接
    //      jedisClient.close()
    //      //d.返回结果
    //      filterLogs
    //    })

    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //获取Redis中的数据,并广播
      //a.获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询数据
      val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
      //c.归还连接
      jedisClient.close()
      //d.广播
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //对RDD操作,进行去重
      rdd.filter(log => {
        !midsBC.value.contains(log.mid)
      })
    })

    //    value1
    //    value2
    value3

  }

  /**
    * 将两次去重之后的结果中的Mid写入Redis,给当天后置批次去重使用
    *
    * @param filterByMid 经过两次去重之后的结果数据
    */
  def saveMidToRedis(filterByMid: DStream[StartUpLog]): Unit = {

    filterByMid.foreachRDD(rdd => {

      //使用分区操作代替单条数据操作
      rdd.foreachPartition(iter => {
        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.遍历iter,写库
        iter.foreach(startLog => {
          val redisKey = s"DAU:${startLog.logDate}"
          jedisClient.sadd(redisKey, startLog.mid)
        })
        //c.归还连接
        jedisClient.close()
      })

    })

  }


}
