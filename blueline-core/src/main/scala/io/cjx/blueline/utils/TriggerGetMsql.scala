package io.cjx.blueline.utils

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class TriggerGetMsql(spark: SparkSession,config: Config) extends Runnable{

  override def run(): Unit = {
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .load().createOrReplaceTempView(config.getString("table_name"))
    println("---------------createOrReplaceTempView Is Over---------------")
  }
}
