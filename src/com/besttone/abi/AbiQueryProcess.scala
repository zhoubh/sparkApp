package com.besttone.abi

import com.besttone.utils.BaseMr
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoubh on 2016/8/30.
  */

case class AbiQuery(mphone: String, start: String, resultcount: String, phonev: String, imei: String, px: String, py: String, imsi: String, code: String, ccode: String, chlno: String, ip: String, reqtime: String, reqtype: String, respcode: String)

object AbiQueryProcess {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val txt = sc.textFile("C:/Users/zhoubh/Downloads/log.log")

    val split_c = "\t"
    val subMap = txt.map {
      line => val br = new BaseMr(line)
        val mphone = br.sub("mobileNO")
        val start = br.sub("start")
        val resultcount = br.sub("resultCount")
        val phonev = br.sub("phoneVersion")
        val imei = br.sub("imei")
        val px = br.sub("poiX")
        val py = br.sub("poiY")
        val imsi = br.sub("imsi")
        val code = br.sub("code")
        val ccode = br.sub("cityCode")
        val chlno = br.sub("channelno")
        val ip = br.sub("requestip")
        val reqtime = br.sub("requesttime")
        val reqtype = br.sub("requesttype")
        val respcode = br.sub("responsecode")
        AbiQuery(mphone, start, resultcount, phonev, imei, px, py, imsi, code, ccode, chlno, ip, reqtime, reqtype, respcode)
    }
    subMap.foreach(println)
  }

}
