package com.besttone

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by zhoubh on 2016/6/28.
  */
object App_Online_Test {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("est").setMaster("local")
    val sc = new SparkContext(conf)
    val txt= sc.textFile("file:/Users/zhoubh/Downloads/apponoff-20160606.log")

    //过滤 不含有 logid 和imei号记录
    val filterTxt=txt.filter(_.contains("imei")).filter(_.contains("logid"))
    //过滤type=3的数据
    val filter3Txt=filterTxt.filter(!_.contains("\\\"type\\\":\\\"3\\\""))
    //获取logid 和imei 号 ，逗号分隔
    val  cleanmap = filter3Txt.map{
      line =>  val data = formatLine(line).split(",")
        (data(0), data(1))
    }

   val rdd =  cleanmap.groupByKey().sortByKey().map(x => (x._1, x._2.toList.sorted))
   rdd.cache()
    rdd.foreach(println)
    sc.stop()
  }


  /**
    * 从每行日志解析出imei和logid
    *
    **/
  def formatLine(line: String): String = {
    val logIdRegex = """"logid":"([0-9]+)",""".r
    val imeiRegex = """\\"imei\\":\\"([A-Za-z0-9]+)\\"""".r
    val logId = getDataByPattern(logIdRegex, line)
    val imei = getDataByPattern(imeiRegex, line)

    //时间取到秒
    imei + "," + logId.substring(0, 14)
  }

  /**
    * 根据正则表达式,查找相应值
    *
    **/
  def getDataByPattern(p: Regex, line: String): String = {
    val result = (p.findFirstMatchIn(line)).map(item => {
      val s = item group 1
      s
    })
    if (!result.isEmpty) {
      result.get
    } else {
      ""
    }
  }

  /**
    * 根据时间字符串获取时间,单位(秒)
    *
    **/
  def getTimeByString(timeString: String): Long = {
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    sf.parse(timeString).getTime / 1000
  }

}
