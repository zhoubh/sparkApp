package com.besttone

import scala.util.matching.Regex

/**
  * Created by zhoubh on 2016/6/28.
  */
object RegExpressTest {

  def main(args: Array[String]): Unit = {


    // 匹配汉字
    val regex0 = ",(.+),".r
    val hanzi = "0,中国人111外国人2222,3,4"
    println("汉字匹配:" + regex0.findFirstIn(hanzi))

    //匹配字母或数字
    val regex =
      """([0-9]+) ([a-z]+)""".r
    val content = "123 scala"
    val regex(num, str) = content
    println(num + ":" + str) //单个匹配，都得匹配上

    //findAllIn、findFirstIn等方法
    val content2 = "hello 123 scala123 456 scala"
    println("findFirstIn:" + regex.findFirstIn(content2))
    println("findAllIn:")
    val regex1 = new Regex("""([0-9]+) ([a-z]+)""")
    val content3 = "1 yy 34 tt 56s 77"
    val allIn = regex1.findAllIn(content3)
    for (regex1(num, str) <- allIn)
      println(num + ":" + str)

    //使用match
    println("使用match：")
    val regex2 = new Regex("""([0-9]+) ([a-z]+)""")
    val content4 = "1234 yy"
    content4 match {
      case regex2(num, str) => println(num + "\t" + str)
      case _ => println("Not matched")
    }


  }

}
