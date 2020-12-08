package com.atguigu.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object JdbcUtil {

  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //执行SQL语句,单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  //执行SQL语句,批量数据插入
  def executeBatchUpdate(connection: Connection, sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      rtn = pstmt.executeBatch()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  //判断一条数据是否存在
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val set: ResultSet = pstmt.executeQuery()
      flag = set.next()
      set.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  //获取MySQL的一条数据
  def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {
    var result: Long = 0L
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  //获取单条数据,封装成JSON对象
  def getJsonDataFromMysql(connection: Connection, sql: String, params: Array[Any]): String = {

    //定义返回值
    var result: JSONObject = new JSONObject()
    //定义预编译SQL
    var pstmt: PreparedStatement = null

    try {
      //预编译SQL
      pstmt = connection.prepareStatement(sql)
      //给占位符添加参数
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      //执行查询,获取数据
      val resultSet: ResultSet = pstmt.executeQuery()
      //解析结果数据
      while (resultSet.next()) {
        result.put("id",resultSet.getString("id"))
        result.put("birthday",resultSet.getString("birthday"))
        result.put("gender",resultSet.getString("gender"))
        result.put("user_level",resultSet.getString("user_level"))
      }

      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result.toString
  }

  //获取MySQL中的黑名单数据
  def getBlackListFromMysql(connection: Connection, sql: String, params: Array[Any]): List[String] = {
    var result: ListBuffer[String] = new ListBuffer[String]()
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result += resultSet.getString(1)
      }
      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result.toList
  }

  def main(args: Array[String]): Unit = {

    val connection: Connection = getConnection

    println(isExist(connection, "select userid from black_list where userid=?;", Array("1")))

  }

}

