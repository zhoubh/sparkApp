package com.besttone.abi

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoubh on 2016/8/19.
  */
object XinLingProcess{

  case class  XinLing(tel_num:String,cnts:Int)

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: XinLingProcess <input> <output>")
      System.exit(1)
    }
     val conf = new SparkConf().setAppName("XinLingProcess").setMaster("local")
     //val conf = new SparkConf().setAppName("XinLingProcess")

    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)

    val txt = sc.textFile(args(0))
    val tel_num= txt.flatMap(line => line.split(",")).filter(_.length()>1)


    val t = tel_num.countByValue()
    println("tel counts:"+t.size)
    val tseq=t.toSeq
    tseq.sortBy(_._2).reverse.take(30).foreach(println)


    val valueDist=t.groupBy(_._2).mapValues(_.size)
    println("Distinct values:" +valueDist)




/**
*
  *
  *
    val result=tel_num.map(line=>(line,1)).reduceByKey(_+_)
    //result.saveAsTextFile(args(1))
    import sqlContext.implicits._
    val xl=result.map(a=>XinLing(a._1,a._2.toInt)).toDF()
    xl.registerTempTable("t_xinling")

    val xl2 = sqlContext.sql("select tel_num,cnts  from t_xinling order by cnts desc")
    xl2.take(10).foreach(println)
    //xl2.collect().foreach(println)
    //xl2.toDF().write.parquet(args(1))

***/

    sc.stop
  }
}
