package io.cjx.blueline.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutputIntra
import io.cjx.blueline.utils.DBMangerPool
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.JavaConversions._
class StructuredMysqlSink extends BaseStructuredStreamingOutputIntra{
  var config: Config = ConfigFactory.empty()
  var Dbpool: DBMangerPool = _
  override def process(df: Dataset[Row]): DataStreamWriter[Row] = ???

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("mysqlurl") && config.hasPath("outputMode")&&config.hasPath("password")&&config.hasPath("user")&&
      config.hasPath("prepareStatement") && config.hasPath("checkpointLocation") match {
      case false => (false, "配置项不正确")
      case true => {
        (true, "")
      }
  }}

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "outputMode" -> "Append",
        "triggerMode" -> "default",
        "maxpoolsize" -> 5,
        "minpoolsize" -> 1
      )
    )
    config = config.withFallback(defaultConfig)
    Dbpool=new DBMangerPool(config.getString("user"),
      config.getString("password"),config.getString("mysqlurl"),config.getInt("maxpoolsize"),config.getInt("minpoolsize"))
  }
}
