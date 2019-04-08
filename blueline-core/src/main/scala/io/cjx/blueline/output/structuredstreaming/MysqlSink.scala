package io.cjx.blueline.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutputIntra
import io.cjx.blueline.utils.{DBMangerPool, MysqlSource}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.JavaConversions._
class MysqlSink extends BaseStructuredStreamingOutputIntra  {
  var config: Config = ConfigFactory.empty()
  var MysqlDBMangerPool:DBMangerPool=_
  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    df.coalesce(2).writeStream.outputMode(config.getString("outputMode")).option("checkpointLocation",config.getString("checkpointLocation")).foreach(
      new MysqlSource(config.getString("mysqlurl"),config.getString("user"),config.getString("password"),config.getString("prepareStatement"),
        MysqlDBMangerPool.getconnection()))
  }
  override def setConfig(config: Config): Unit = this.config=config
  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("mysqlurl") && config.hasPath("outputMode")&&config.hasPath("password")&&config.hasPath("user")&&
      config.hasPath("prepareStatement") && config.hasPath("checkpointLocation") match {
      case false => (false, "配置项不正确")
      case true => {
        (true, "")
      }
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "outputMode" -> "Append",
        "triggerMode" -> "default",
        "maxpoolsize" -> 20,
        "minpoolsize" -> 1
      )
    )
    config = config.withFallback(defaultConfig)
    logInfo("===== start init db pool")
    MysqlDBMangerPool=new DBMangerPool(config.getString("user"),
      config.getString("password"),config.getString("mysqlurl"),config.getInt("maxpoolsize"),config.getInt("minpoolsize"))
    logInfo("===== end init db pool")
  }
}
