package com.besttone.abi

import com.besttone.ip.IpSearch
import com.besttone.utils.{LatnUtil, RegionUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by zhoubh on 2016/8/10.
  */
object GuaduanProcess {
   case class GuaDuan(ip:String, types: String, count: Int)
  val split_c="\t"

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: GuaduanProcess <input> <output>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)

    val txt = sc.textFile(args(0))
    val result=txt.filter(_.contains("GET")).filter(_.contains("302"))
    val r1=result.map{
      line => val s = getDuaduan(line)
        (s,1)

    }
    val rdd = r1.reduceByKey(_ + _)
    import sqlContext.implicits._
    rdd.take(10).foreach(println)
    //rdd.saveAsTextFile(args(1))

    val gd = rdd.map(x => x._1 + split_c + x._2).map(_.split(split_c)).map(a => GuaDuan(a(0), a(1), a(2).toInt )).toDF()
    gd.registerTempTable("gua_duan")

    val r2 = sqlContext.sql("select *  from gua_duan where length(types)=7 and   types='2g0nl00'")
    r2.collect().foreach(println)

    sc.stop



  }

  def getDuaduan(line:String):String={
    val guaduanRegex ="""GET /(.+) HTTP/1.1" 302""".r
    val ipRegex="""- (\d+.\d+.\d+.\d+) -""".r

    val guaduan = getDataByPattern(guaduanRegex, line)
    val ip=getDataByPattern(ipRegex, line)


    ip+"\t"+guaduan
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


}
