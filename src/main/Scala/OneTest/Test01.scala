package OneTest

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

//曹永宏

object Test01 {
  def main(args: Array[String]): Unit = {

    //    if(args.length != 2){
    //      println("目录不匹配，退出程序")
    //      sys.exit()
    //    }
    //    val Array(inputPath,outputPath,dirPath,stopPath)=args

    var list: List[String] = List()

    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    //获取上下文
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //读取数据
    val df: RDD[String] = sc.textFile("dir/json.txt")

    val data: mutable.Buffer[String] = df.collect().toBuffer

    for(i <- 0 until data.length) {
      val str: String = data(i).toString

      // 解析json串
      val jsonparse: JSONObject = JSON.parseObject(str)

      // 判断状态是否成功
      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""

      // 接下来解析内部json串，判断每个key的value都不能为空
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return null

      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()
      // 循环输出
      for (item <- poisArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("type"))
        }
      }

      list:+=buffer.mkString(";")
    }

    val result: List[(String, Int)] = list.flatMap(x => x.split(";"))
      .map(x => ( x, 1)).groupBy(x => x._1).mapValues(x => x.size).toList
    result.foreach(println)

  }
}
