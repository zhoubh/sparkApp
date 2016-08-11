package com.besttone

/**
  * Created by zhoubh on 2016/6/28.
  */
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * 用户在线时长和登录次数统计
  */
object UserOnlineAnalysis {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: UserOnlineAnalysis <input> <output>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("UserOnlineAnalysis").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))

    //剔除type等于3的数据
    val notContainsType3 = data.filter(!_.contains("\\\"type\\\":\\\"3\\\""))

    //过滤logid或imei不存在的数据
    val cleanData = notContainsType3.filter(_.contains("logid")).filter(_.contains("imei"))

    val cleanMap = cleanData.map {
      line =>
          val data = formatLine(line).split(",")
        (data(0), data(1))
    }

    val rdd = cleanMap.groupByKey().sortByKey().map(x => (x._1, x._2.toList.sorted))

    rdd.cache()

    //导出明细
    exportDetailData(rdd, args(1) + "/detail")

    //导出统计
    exportSumData(rdd, args(1) + "/sum")


    rdd.unpersist()

    sc.stop()

  }

  /**
    * 导出用户在线时长和登录次数统计结果
    * 存储结构:(IMEI,登录次数,在线时长(秒))
    *
    **/
  def exportSumData(map: RDD[(String, List[String])], output: String): Unit = {
    val result = map.map {
      x =>
        //登录次数,默认登录1次
        var logNum: Int = 1
        //在线时长(秒)
        var totalTime: Long = 0

        val len = x._2.length

        for (i <- 0 until len) {
          if (i + 1 < len) {
            val nowTime = getTimeByString(x._2(i))
            val nextTime = getTimeByString(x._2(i + 1))
            val intervalTime = nextTime - nowTime
            if (intervalTime < 60 * 10) {
              totalTime += intervalTime
            } else {
              logNum += 1
            }
          }

        }
        //输出ime,登录次数,总时长(秒)
        (x._1, logNum, totalTime)
    }

    result.saveAsTextFile(output)
  }

  /**
    * 导出用户在线时长和首次登录时间
    * 存储结构:(IMEI,首次登录时间,在线时长(秒))
    *
    **/
  def exportDetailData(map: RDD[(String, List[String])], output: String): Unit = {
    val result = map.flatMap {
      x =>
        val len = x._2.length
        val array = new Array[(String, String, Long)](len)
        for (i <- 0 until len) {
          if (i + 1 < len) {
            val nowTime = getTimeByString(x._2(i))
            val nextTime = getTimeByString(x._2(i + 1))
            val intervalTime = nextTime - nowTime
            if (intervalTime < 60 * 10) {
              array(i) = (x._1, x._2(i), intervalTime)
            } else {
              array(i) = (x._1, x._2(i), 0)
            }
          } else {
            array(i) = (x._1, x._2(i), 0)
          }

        }
        array
    }
    result.saveAsTextFile(output)
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
