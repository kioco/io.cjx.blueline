package io.cjx.blueline.input

import java.util.concurrent.{Executors, TimeUnit}

import io.cjx.blueline.utils.TriggerGetMsql
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Mysql extends Jdbc{
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .load()
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    if (config.hasPath("checkinterval")){
      val service = Executors.newSingleThreadScheduledExecutor()
      service
        .scheduleAtFixedRate(
          new TriggerGetMsql(spark,config)
          ,"60".toLong,config.getString("checkinterval").toLong,TimeUnit.SECONDS)
      logDebug("------------Trigger Mysql is start-----------")
    }
  }
}
