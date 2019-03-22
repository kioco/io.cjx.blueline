package io.cjx.blueline.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Sql extends BaseFilter{
  var conf: Config = ConfigFactory.empty()
  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    df.createOrReplaceTempView(this.conf.getString("table_name"))
    logInfo("=================SQL DATA:" + conf.getString("sql"))
    spark.sql(conf.getString("sql"))
  }

  override def setConfig(config: Config): Unit = this.conf=config

  override def getConfig(): Config = this.conf

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("table_name") && conf.hasPath("sql") match {
      case true => (true, "")
      case false => (false, "please specify [table_name] and [sql]")
    }
  }
}

