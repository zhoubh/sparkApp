package com.besttone.abi

import com.besttone.utils.BaseMr
import org.apache.spark.{SparkConf, SparkContext}

// 从Java Map到Scala Map，适用于可变树形映射
import scala.collection.JavaConversions.mapAsScalaMap

/**
  * Created by zhoubh on 2016/8/2.
  */
object AbilityProcess {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: AbilityProcess <input> <output>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val txt = sc.textFile("C:/Users/zhoubh/Downloads/ability.bz2")
    val txt = sc.textFile(args(0))
    val split_c = "\t"
    val subMap = txt.map {
      line => val br = new BaseMr(line)
        val subMap = br.getSubMap()

        val day_id = subMap.get("day_id").toString

        val hour_id = subMap.get("hour_id").toString

        val requestIp = br.sub("requestip");
        //val cityCode = subMap.get("cityCode").toString
        //val cityName = br.sub("city_name");
        val queryNum = br.sub("queryNum");
        val regionCode = subMap.get("regionCode").toString
        val requesttype = subMap.get("requesttype").toString
        val channelno = subMap.get("channelno").toString
        val imsi = br.sub("imsi")
        val subjectNum = subMap.get("subjectNum").toString

        val responsecode = br.sub("responsecode")
        val imei = subMap.get("imei").toString
        val latnCode = subMap.get("latnCode").toString

        //        if (day_id.equals("noday_id"))
        //          return;
        //        if (day_id.equals("noday_id"))
        //          return;
        //        if (!hour_id.startsWith("0"))
        //          return;
        //        if (!hour_id.startsWith("1"))
        //          return;
        //        if (!hour_id.startsWith("2"))
        //          return;
        //        if (subjectNum.equals("return"))
        //          return;
        //scala 没有return  最后一个值即为返回值

        day_id + split_c + hour_id + split_c + requestIp + split_c + channelno + split_c + imei + split_c + imsi + split_c + latnCode + split_c + regionCode + split_c + queryNum + split_c + subjectNum + split_c + requesttype + split_c + responsecode

    }
    //subMap.foreach(println)
    val result=subMap.filter(!_.contains("return")).map((_,1)).reduceByKey(_+_)
    //result.foreach(println)
   // resut.saveAsTextFile(args(1))

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    result.map(x => x._1 + split_c + x._2).saveAsTextFile(args(1)+"/ability_txt")
    /**
    *

    val abi= result.map(x => x._1 + split_c + x._2).map(_.split(split_c)).map(p => Ability(p(0), p(1), p(2), p(3), p(4), p(5),p(6),p(7),p(8),p(9),p(10),p(11), p(12).trim.toInt)).toDF

    abi.registerTempTable("abi")
    val pv = sqlContext.sql("SELECT count(*) cnts,sum(cnts) pv ,count(distinct imei) uv,count(distinct hour_id) hr FROM abi ")
    pv.map(t => "cnts: " + t(0) + " pv:" + t(1) + " uv:" + t(2) + " hour:" + t(3)).collect().foreach(println)

    abi.write.parquet(args(1)+"/ability")
    val parquetFile = sqlContext.read.parquet(args(1) + "/ability")
    parquetFile.registerTempTable("abi2")
    val pv2 = sqlContext.sql("SELECT count(*) cnts,sum(cnts) pv ,count(distinct imei) uv,count(distinct hour_id) hr FROM abi2 ")
    pv2.map(t => "cnts: " + t(0) + " pv:" + t(1) + " uv:" + t(2) + " hour:" + t(3)).collect().foreach(println)
      * */
    sc.stop()
  }
  // 能力模型
  case class Ability(day_id:String, hour_id:String ,requestIp:String,channelno:String,imei:String,imsi:String,latnCode:String,regionCode:String,queryNum:String,subjectNum:String,requesttype:String,responsecode:String,cnts:Int)


//   def getAbilityMap(line:String):String={
//
//
//   }

}
