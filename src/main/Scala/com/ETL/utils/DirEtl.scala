package com.ETL.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DirEtl {

  def main(args: Array[String]): Unit = {


    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    //创建入口
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取文件
    val log = sc.textFile("E:/app_dict.txt")
      .map(line => {
        val fields = line.split("\\s").filter(x =>x.length>=5)
        val id = fields(4)
        val name = fields(1)
        (id,name)
      })



    log.saveAsTextFile("E:/11111111111111111111111")


    sc.stop()

  }


}