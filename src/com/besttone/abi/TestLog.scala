package com.besttone.abi

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoubh on 2016/8/30.
  */
object TestLog {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestLog").setMaster("local")
    //val conf = new SparkConf().setAppName("XinLingProcess")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val txt = sc.textFile("C:\\Users\\zhoubh\\Downloads\\log")
    val tel_num = txt.filter(_.contains("labourRace/getValidationCode.do"))
    val parsed = tel_num.map(line => parse(line))
    parsed.cache()

    val matchCounts = parsed.countByValue()
    val matchCountsSeqOrder = matchCounts.toSeq.sortBy(_._2).reverse
    matchCountsSeqOrder.take(100).foreach(println)
    sc.stop
  }


  def parse(line: String) = {
    //    val pieces = line.split("- -")
    //    val ip = pieces(0).replace(" 172.16.0.16 ","")
    val pieces = line.split(" ")
    val ip = pieces(0)
    ip
  }
}