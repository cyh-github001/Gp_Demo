package com.ETL.utils

import com.utils.JedisConnectionPool
import org.apache.spark.{SparkConf, SparkContext}

object APp2Jedis {

  /**
    * 将字段文件数据，存储到redis中
    */

    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("app").setMaster("local[*]")
      val sc = new SparkContext(conf)
      // 读取字段文件
      val dict = sc.textFile("E:\\app_dict.txt")
      // 读取字段文件
      dict.map(_.split("\t", -1))
        .filter(_.length >= 5).foreachPartition(arr => {
        val jedis = JedisConnectionPool.getConnection()
        arr.foreach(arr => {
          jedis.set(arr(4), arr(1))
        })
        jedis.close()
      }
      )
      sc.stop()
    }
  }

