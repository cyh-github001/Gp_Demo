package OneTest

import breeze.numerics.log
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{GenTraversableOnce, mutable}
import scala.collection.mutable.ListBuffer

object Test {

  def main(args: Array[String]): Unit = {

//    if(args.length != 2){
//      println("目录不匹配，退出程序")
//      sys.exit()
//    }
//    val Array(inputPath,outputPath,dirPath,stopPath)=args
    // 创建上下文
var list: List[List[String]] = List()
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df= sc.textFile("dir/json.txt")


    val res = Utils.Analysis(df)

        val ress: List[(String, Int)] = list.flatMap(x => x)
          .filter(x => x != "[]").map(x => (x, 1))
          .groupBy(x => x._1)
          .mapValues(x => x.size).toList.sortBy(x => x._2)


        ress.foreach(x => println(x))

    sc.stop()


  }

}
