package com.besttone.app

import java.text.SimpleDateFormat

import com.besttone.ip.IpSearch
import com.besttone.utils.{LatnUtil, RegionUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by zhoubh on 2016/7/18.
  */
object ApponoffProcess {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: ApponoffProcess <input> <output>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val txt = sc.textFile("C:/Users/zhoubh/Downloads/apponoff-20160606.bz2")
    val txt = sc.textFile(args(0))


    //过滤 不含有 logid 和imei号记录  替换制表符，作为解析分隔符
    val filterTxt = txt.filter(_.contains("imei")).filter(_.contains("logid")).filter(_.contains("requestip")).map(_.replace("\t", ""))
    //获取logid 和imei 号 ，逗号分隔
    val cleanmap = filterTxt.map {
      line => (formatLine(line), 1)
    }
    val rdd = cleanmap.reduceByKey(_ + _)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    val app2 = rdd.map(x => x._1 + split_c + x._2).map(_.split(split_c)).map(p => Apps(p(0), p(1), p(2), p(3), p(4), p(5), p(6).trim.toInt)).toDF
    app2.registerTempTable("app")
    val pv = sqlContext.sql("SELECT count(*) cnts,sum(cnts) pv ,count(distinct imei) uv,count(distinct hour) hr FROM app ")
    pv.map(t => "cnts: " + t(0) + " pv:" + t(1) + " uv:" + t(2) + " hour:" + t(3)).collect().foreach(println)

    app2.write.parquet(args(1) + "/app2.parquet")
    val parquetFile = sqlContext.read.parquet(args(1) + "/app2.parquet")
    parquetFile.registerTempTable("parquetFile")
    val teenagers = sqlContext.sql("SELECT count(*) cnts,sum(cnts) pv ,count(distinct imei) uv,count(distinct hour) hr FROM parquetFile")
    teenagers.map(t => "cnts: " + t(0) + " pv:" + t(1) + " uv:" + t(2) + " hour:" + t(3)).collect().foreach(println)

    sc.stop()
  }

  //解析的app
  case class Apps(imei: String, day_id: String, hour: String, ip: String, region_code: String, latn_code: String, cnts: Int)

  //分隔符 为制表符
  val split_c = "\t"


  /**
    * 原始日志
    * String keyValue = logid    requestIp    deviceType    padOrPhone    version    channelno    type    imei imsi day_id hour_id regionCode cityCode opt
    * 2016-07-18 00:00:00 {"areacode":"浙江省舟山市","datatime":"65317","logid":"201607180000001128850920","requestinfo":"{\"entertime\":\"2015-12-18 03:34:28\",\"imsi\":\"460036250802291\",\"ty
    * pe\":\"0\",\"channelno\":\"1213\",\"version\":\"7.3.3.0ctch1\",\"imei\":\"A0000049D17336\"}","requestip":"60.185.196.207","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0,\"ms
    * g\":\"操作成功\"}"}
    */

  def formatLine(line: String): String = {
    val logIdRegex = """"logid":"([0-9]+)",""".r
    val imeiRegex = """\\"imei\\":\\"([A-Za-z0-9]+)\\"""".r
    val requestipRegex =""""requestip":"(\d+\.\d+\.\d+\.\d+)"""".r
    val logId = getDataByPattern(logIdRegex, line)
    val imei = getDataByPattern(imeiRegex, line)
    val requestip = getDataByPattern(requestipRegex, line)

    val regioncode = {
      val s = {
        if (requestip.length >= 9)
          getRegionByIp(requestip)
        else
          "000000" + split_c + "000000"
      }
      s
    }
    //时间取到小时
    imei + split_c + logId.substring(0, 8) + split_c + logId.substring(8, 10) + split_c + requestip + split_c + regioncode
  }

  /**
    * 根据IP或者用户所在的省份和地市
    **/
  def getRegionByIp(ip: String): String = {

    val finder = IpSearch.getInstance()
    val result = finder.Get(ip)
    val splits = result.split("\\|")

    val listCity = List("澳门", "台湾", "香港", "海南", "天津", "北京", "重庆")
    val listConty = List("济源", "仙桃", "潜江", "天门", "神农架", "阿拉尔", "石河子")

    val s = {
      if (splits.length == 11) {
        var province = splits(2)
        var city = splits(3)
        val conty = splits(4)

        if (listCity.contains(province))
          city = province
        else if (listConty.contains(conty))
          city = conty
        else if ("".equals(city))
          city = province

        if (province.length > 2)
          province = province.substring(0, 2)

        val s1 = {

          getRegionCodeByName(province, "prvn") + split_c +
            getRegionCodeByName(city, "latn")

        }
        s1
      }

      else
        "000000" + split_c + "000000"
    }
    s
  }


  /**
    * 根据省份地市名或者编码
    **/

  def getRegionCodeByName(name: String, types: String): String = {
    val s = {
      if (types.equals("latn"))
        LatnUtil.getRegionCode(name)
      else
        RegionUtil.getRegionCode(name)
    }
    s
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
    result.getOrElse("")
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
