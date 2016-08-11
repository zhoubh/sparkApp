package com.besttone

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * 用户在线时长和登录次数统计
  */
object App {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val dd = sc.textFile("file:/Users/zhoubh/Downloads/apponoff-20160606.log")

    //过滤数据,如果logid和imei不存在,丢弃
    val d1 = dd.filter(_.contains("logid")).filter(_.contains("imei"))

    //获取imei,logid值,用","分隔
    val d2 = d1.map({
      line =>
        val data = getInfo(line).split(",")
        (data(0), Iterable(data(1)))
    })

    //构造(key,value,value...)值,key为imei,value为logid
    val d3 = d2.flatMapValues(v => v).groupByKey()
    val sf:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

    val d4 = d3.map {
      case (imei, logIds) =>
        //        print(logIds)

        //获取首次登录logid
        val firstLog = logIds.head
        //剔除首次登录,方便比较

        val subList = logIds.drop(1)

        //登录次数,默认登录1次
        var logNum: Int = 1
        //在线时长(秒)

        var totalTime: Long = 1

        //转换成int值,便于比较
        var time = sf.parse(firstLog)

        //遍历剩余logid,和首次做比较
        for (x <- subList) {
          val nextTime =  sf.parse(x)
          //在线时长统计:总时间+下个logid时间-上个logid时间
          val intervalTime = (nextTime.getTime - time.getTime)/1000
          totalTime = totalTime + intervalTime

          //如果间隔时间大于15分钟,则登录次数加1
          if (intervalTime >= 60*15) {
            logNum = logNum + 1
            time = nextTime
          }
        }
        //输出ime,登录次数,总时长(秒)
        (imei, logNum, totalTime)
    }

    d4.foreach(println)
    d4.saveAsTextFile("file:/Users/zhoubh/Downloads/apponoff.out")
    sc.stop()

  }


  def getInfo(line: String): String = {
    val logIdRegex = """"logid":"([0-9]+)",""".r
    val imeiRegex = """\\"imei\\":\\"([A-Za-z0-9]+)\\"""".r
    val logId = getResult(logIdRegex, line)
    val imei = getResult(imeiRegex, line)

    //时间取到秒
    imei + "," + logId.substring(0, 14)
  }

  def getResult(p: Regex, line: String): String = {
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

}
