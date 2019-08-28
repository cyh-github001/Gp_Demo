package OneTest

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Utils {

  /**
    * 解析数据
    */
  def Analysis(df: RDD[String]):Any= {

    var list: List[List[String]] = List()
    val logs: mutable.Buffer[String] = df.collect().toBuffer

    for (i <- 0 until logs.length) {
      val str: String = logs(i).toString

      val jsonparse: JSONObject = JSON.parseObject(str)

      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""

      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val pois= regeocodeJson.getJSONArray("pois")
      if (pois == null || pois.isEmpty) return null

      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()
      // 循环输出
      for (item <- pois.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      val list1: List[String] = buffer.toList

      list :+= list1
    }




  }

}