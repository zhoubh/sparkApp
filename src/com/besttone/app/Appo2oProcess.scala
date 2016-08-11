package com.besttone.app

import com.besttone.ip.IpSearch
import com.besttone.utils.{LatnUtil, RegionUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by zhoubh on 2016/8/4.
  */

/**
  *
  * 2015-08-03 08:00:40 {"areacode":"未知国家","logid":"201508030800401254875709","requestinfo":"{\"ID\":\"71\",\"NAME\":\"114火车票\",\"NOTE\":\"114火车票\",\"channelno\":\"1006\",\"imei\":\"A000004FF85E06\",\"queryNum\":\"\"}","requestip":"172.17.225.150","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0}"}
  * 2015-08-03 08:02:08 {"areacode":"未知国家","logid":"201508030802081147615297","requestinfo":"{\"ID\":\"71\",\"NAME\":\"114火车票\",\"NOTE\":\"114火车票\",\"channelno\":\"1018\",\"imei\":\"865155020003261\",\"queryNum\":\"\"}","requestip":"172.17.225.150","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0}"}
  * 2016-08-01 00:00:15 {"areacode":"四川省成都市","logid":"201608010000151795839308","requestinfo":"{\"ID\":\"huangyedibuanniu\",\"NAME\":\"黄页\",\"NOTE\":\"底部按钮\",\"channelno\":\"1006\",\"imei\":\"a0000059bbc49b\",\"queryNum\":\"18982282578\"}","requestip":"182.149.189.87","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0}","userAgent":"Dalvik/2.1.0 (Linux; U; Android 6.0; HUAWEI NXT-CL00 Build/HUAWEINXT-CL00)"}
  * 2016-08-01 00:00:21 {"areacode":"海南省三亚市","logid":"201608010000211176560726","requestinfo":"{\"ID\":\"chahaochahao\",\"NAME\":\"查号\",\"NOTE\":\"查号\",\"channelno\":\"1018\",\"queryNum\":\"\"}","requestip":"140.240.67.235","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0}","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.0.2; SM-T550 Build/LRX22G)"}
  * 2016-08-01 00:00:25 {"areacode":"山东省","logid":"201608010000251762414193","requestinfo":"{\"ID\":\"bohaobohao\",\"NAME\":\"拨号\",\"NOTE\":\"拨号\",\"channelno\":\"1002\",\"imei\":\"A00000488E3485\",\"queryNum\":\"\"}","requestip":"39.89.96.132","responseinfo":"{\"errorCode\":\"000000\",\"flag\":true,\"id\":0}","userAgent":"Dalvik/1.6.0 (Linux; U; Android 4.1.2; SCH-W789 Build/JZO54K)"}
  *
  **/
object Appo2oProcess {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: Appo2oProcess <input> <output>")
      System.exit(1)
    }

    val sc = sparkContext("appo2o_log_process", true)
    val sqlCon = sqlContext(sc)
    //val hiveCon = hiveContext(sc)

    val txt = sc.textFile(args(0))
    val filterTxt = txt.filter(_.contains("imei")).filter(_.contains("logid")).filter(_.contains("requestip"))
    val o2o = filterTxt.map {
      line => val s = getAppo2o(line)
        (s, 1)
    }
    o2o.take(10).foreach(println)
    val rdd = o2o.reduceByKey(_ + _)
    import sqlCon.implicits._
    //import hiveCon.implicits._

    val appo2o = rdd.map(x => x._1 + split_c + x._2).map(_.split(split_c)).map(a => Appo2o(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), a(11).toInt)).toDF()

    appo2o.registerTempTable("appo2o")

    //appo2o.write.parquet(args(1))
     val result = sqlCon.sql("select  sum(cnts) pv , count(distinct imei) uv ,day_id from appo2o group by day_id")
    result.collect().foreach(println)

     //hiveCon.sql("use default")
    //hiveCon.sql("drop table if exists tmp_appo2olog" )
    //val result2=hiveCon.sql( "select  sum(cnts) pv , count(distinct imei) uv ,day_id　from appo2o group by day_id")
    //result2.collect().foreach(println)
    sc.stop()
  }

  def sparkContext(jobName: String = "spark_test_job", isLocal: Boolean = true): SparkContext = {
    val conf = if (isLocal) {
      new SparkConf().setAppName(jobName).setMaster("local")
    }
    else {
      new SparkConf().setAppName(jobName)
    }
    new SparkContext(conf)
  }

  def sqlContext(sc: SparkContext): SQLContext = {
    new SQLContext(sc)
  }

  def hiveContext(sc: SparkContext): HiveContext = {
    new HiveContext(sc)
  }

  /**
    * 解析O2O日志里的相应字段
    *
    **/
  def getAppo2o(line: String): String = {

    val line1 = line.replace("\"", "").replace("\\", "").replace("{", "").replace("\t", "")
    val logidRegex ="""logid:(\d+),""".r
    val idRegex ="""ID:([A-Za-z0-9]+),""".r
    val nameRegex ="""NAME:(.+),NOTE""".r
    val noteRegex ="""NOTE\:(.+),channelno""".r
    val channelnoRegex ="""channelno:(\d+),""".r
    val imeiRegex ="""imei:([a-zA-Z0-9]+),""".r
    val querynumRegex ="""queryNum:(\d?)},""".r
    val requestipRegex ="""requestip:(\d+\.\d+\.\d+\.\d+),""".r

    val logid = getDataByPattern(logidRegex, line1)
    val day_id = logid.substring(0, 8)
    val hour_id = logid.substring(8, 10)
    val id = getDataByPattern(idRegex, line1)
    val name = getDataByPattern(nameRegex, line1)
    val note = getDataByPattern(noteRegex, line1)
    val channelno = getDataByPattern(channelnoRegex, line1)
    val imei = getDataByPattern(imeiRegex, line1)
    val querynum = getDataByPattern(querynumRegex, line1)
    val requestip = getDataByPattern(requestipRegex, line1)


    val regioncode = {
      val s = {
        if (requestip.length >= 9)
          getRegionByIp(requestip)
        else
          "000000" + split_c + "000000"
      }
      s
    }

    day_id + split_c + hour_id + split_c + id + split_c + name + split_c + note + split_c + channelno + split_c + imei + split_c + requestip + split_c + regioncode + split_c + querynum
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

  case class Appo2o(day_id: String, hour_id: String, id: String, name: String, note: String, channelno: String, imei: String, requestip: String, region_code: String, latn_code: String, querynum: String, cnts: Int)

  //分隔符 为制表符
  val split_c = "\t"

}
