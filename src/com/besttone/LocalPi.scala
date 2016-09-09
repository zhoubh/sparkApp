package com.besttone

/**
  * Created by zhoubh on 2016/6/27.
  */
import scala.math.random

import org.apache.spark._
import org.apache.spark.SparkContext._

object LocalPi {
  def main(args: Array[String]) {
    var count = 0
    for (i <- 1 to 100000) {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) count += 1
    }
    println("Pi is roughly " + 4 * count / 100000.0)
    val s="abc,\td\tefg"
    val s1=s.substring(1,s.length -1)
    val s2=s.replace("\t","")
    println(s)
    println(s1)
    println(s2)
   }
}
