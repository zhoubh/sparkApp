package com.besttone.abi

import com.besttone.ip.IpSearch
import com.besttone.utils.{BaseMr, LatnUtil, RegionUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by zhoubh on 2016/9/9.
  */
object YuYin114 {

  case class YuYin(ip: String, cnts: Int)
  case class TXZL(regioncode:String, latncode:String,ip: String,  cnts: Int)

  //分隔符 为制表符
  val split_c = "\t"

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: YuYin114 <input> <output>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("114 YuYin.").setMaster("local")
    //val conf = new SparkConf().setAppName("XinLingProcess")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val txt = sc.textFile(args(0))

    //    val yuyin = txt.map(x =>parse(x)).toDF
    //     yuyin.registerTempTable("yuyin")
    //    val result = sqlContext.sql("select count(distinct ip) uv, sum(cnts) pv from yuyin")
    //    result.map(t => "sum_uv: " + t(0) + " sum_pv:" + t(1) ).collect().foreach(println)

    /** **
      * val ip = txt.filter(_.contains("labourRace/signed.do")).map(x =>parse1(x))
      * val t = ip.countByValue()
      * println("ip counts:" + t.size)
      * val tseq = t.toSeq
      * tseq.sortBy(_._2).reverse.take(100).foreach(println)
      * val valueDist = t.groupBy(_._2).mapValues(_.size)
      * println("Distinct values:" + valueDist)
      * */
    //val filterTxt =txt.filter(_.contains("labourRace/signed.do"))

      // GET /0USzk37 HTTP/1.1" 302    取114语音数据 0开头记录
    val filterTxt =txt.filter(_.contains("GET /0")).filter(_.contains("302"))

    val result = filterTxt.map {
      line => val s = parse1(line)
        (s,1)
    }

//    全部统计 不去掉114语音记录
//  val result = txt.map {
//       line => val s = parse1(line)
//         (s,1)
//     }
    result.take(10).foreach(println)

    val txzl = result.map(x => x._1 + split_c + x._2).map(_.split(split_c)).map(a => TXZL(a(0), a(1), a(2), a(3).toInt)).toDF()
    txzl.registerTempTable("txzl")
    val re = sqlContext.sql("select regioncode, count(ip),count(distinct ip),sum(cnts)  from txzl group by regioncode order by 1")
    re.collect().foreach(println)


    sc.stop

  }

  def parse(line: String):YuYin = {
    val pieces = line.split("- -")
    val ip = pieces(0).replace(" ", "").replace("-", "")

    //ip
    YuYin(ip,1)
  }

  def parse1(line: String):String = {
    val requestipRegex ="""- (\d+\.\d+\.\d+\.\d+) - -""".r
    val ip = getDataByPattern(requestipRegex, line)
    val regioncode = {
      val s = {
        if (ip.length >= 9)
          getRegionByIp(ip)
        else
          "000000" + "\t" + "000000"
      }
      s
    }
    regioncode+"\t"+ip
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

          getRegionCodeByName(province, "prvn") + "\t" +
            getRegionCodeByName(city, "latn")

        }
        s1
      }

      else
        "000000" + "\t" + "000000"
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


}
