package com.besttone.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoubh on 2016/8/3.
  */
object TestMysql {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mysql").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.hive.HiveContext(sc)
    val jdbcDF=sqlContext.read.options(jdbcMap).format("jdbc").load
    //使用default库
    sqlContext.sql("use default")
    jdbcDF.registerTempTable("zbh_test")

    val t1= sqlContext.sql("select sum(pv_cnts) pv from zbh_test")
    t1.registerTempTable("temp2")
    val t2=sqlContext.sql("select * from temp2")
    //create table in hive
    // sqlContext.sql("create table test_zbh1111 as select * from temp2")
    //write to hive
    sqlContext.sql("insert into  test_zbh1111  select * from temp2")

    //read data  from hive
    val result= sqlContext.sql("select * from test_zbh1111")
     result.foreach(println)


    sqlContext.dropTempTable("zbh_test")
    sqlContext.dropTempTable("temp2")

    sc.stop()
  }

  val mysqlDriverUrl = "jdbc:mysql://url"
  val tableName = "zbh_test"
  val jdbcMap = Map("url" -> mysqlDriverUrl,
    "user" -> "root",
    "password" -> "password",
    "dbtable" -> tableName,
    "driver" -> "com.mysql.jdbc.Driver")
  
}
