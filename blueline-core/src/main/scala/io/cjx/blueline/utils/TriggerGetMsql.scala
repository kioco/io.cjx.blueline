package io.cjx.blueline.utils

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class TriggerGetMsql() extends Runnable{
  var spark_ : SparkSession=_
  var config_ : Config=_
  def this(spark: SparkSession,config: Config){
    this()
    this.spark_ =spark
    this.config_ =config
  }
  override def run(): Unit = {
    spark_.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", config_.getString("url"))
      .option("dbtable", config_.getString("table"))
      .option("user", config_.getString("user"))
      .option("password", config_.getString("password"))
      .load().createOrReplaceTempView(config_.getString("table_name"))
    println("---------------createOrReplaceTempView Is Over---------------")
  }
}
