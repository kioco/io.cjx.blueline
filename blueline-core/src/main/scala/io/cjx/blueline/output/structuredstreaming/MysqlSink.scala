package io.cjx.blueline.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutputIntra
import io.cjx.blueline.utils.MysqlSource
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row}

class MysqlSink extends BaseStructuredStreamingOutputIntra{
  var config: Config = ConfigFactory.empty()
  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    df.writeStream.outputMode(config.getString("outputMode")).option("checkpointLocation",config.getString("checkpointLocation")).foreach(
      new MysqlSource(config.getString("mysqlurl"),config.getString("user"),config.getString("password"),config.getString("prepareStatement")))
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
}
