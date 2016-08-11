package com.besttone.utils
/**
  * Created by zhoubh on 2016/7/22.
  */
import java.sql.{Connection, DriverManager}
import java.util.Properties
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry}
import org.apache.spark.{Logging}


private  object JDBCRDD extends Logging {
  def getConnector(driver: String, url: String, properties: Properties): () => Connection = {
    () => {
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case e: ClassNotFoundException =>
          logWarning(s"Couldn't find class $driver", e)
      }
      DriverManager.getConnection(url, properties)
    }
  }
}
