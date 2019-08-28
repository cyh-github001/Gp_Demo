package com.Tags

import ch.hsr.geohash.GeoHash
import com.ETL.utils.{AmapUtil, Tag, Utils2Type}
import com.utils.JedisConnectionPool
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 商圈标签
  * 思路分析：
  * 1，定义一个方法，将传入的row参数转换类型，定义一个集合List，并且提取出（经度）"long"和（纬度）"lat"
  * 2，对获取到的经纬度进行过滤，取相对应范围内的经纬度
  * 3，定义一个方法，将long和lat传进去，用GeoHash方法将long和lat转换成Geo字符串
  * 4，再定义一个方法，获取Redis连接，将GeoHash作为Key传进去get对应的value(商圈信息)
  * 5，判断通过GeoHash取出来的值是否为空，如果为空的话就调用AmapUtil方法通过经纬度获取商圈信息
  * 6，定义一个方法，将GeoHash和Business传进去，获取Redis连接，将GeoHash作为Key，
  * Bussiness作为Value,Set(存)到Redis中去，返回商圈信息的值
  * 7，判断缓存中是否有此商圈，如果有的话，将商圈信息按逗号切开，以List((f,1))的形式进行遍历，
  * 返回List
  */
object BusinessTag extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 获取经纬度，过滤经纬度
    if(Utils2Type.toDouble(long)>= 73.0 &&
      Utils2Type.toDouble(long)<= 135.0 &&
      Utils2Type.toDouble(lat)>=3.0 &&
      Utils2Type.toDouble(lat)<= 54.0){
      // 先去数据库获取商圈
      val business = getBusiness(long.toDouble,lat.toDouble)
      // 判断缓存中是否有此商圈
      if(StringUtils.isNotBlank(business)){
        val lines = business.split(",")
        lines.foreach(f=>list:+=(f,1))
      }
      //      list:+=(business,1)
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long:Double,lat:Double):String={
    // 转换GeoHash字符串
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    // 去数据库查询
    var business = redis_queryBusiness(geohash)
    // 判断商圈是否为空
    if(business ==null || business.length == 0){
      // 通过经纬度获取商圈
      business = AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble)
      // 如果调用高德地图解析商圈，那么需要将此次商圈存入redis
      redis_insertBusiness(geohash,business)
    }
    business
  }

  /**
    * 获取商圈信息
    */
  def redis_queryBusiness(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到redis
    */
  def redis_insertBusiness(geoHash:String,business:String): Unit ={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geoHash,business)
    jedis.close()
  }

}
