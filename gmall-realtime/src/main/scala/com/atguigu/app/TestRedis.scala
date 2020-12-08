package com.atguigu.app

import com.atguigu.utils.RedisUtil
import redis.clients.jedis.Jedis

object TestRedis {

  def main(args: Array[String]): Unit = {

    val client: Jedis = RedisUtil.getJedisClient

    client.set("中文","股票")


  }

}
