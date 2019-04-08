package io.cjx.blueline.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutput
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row}

class StructuredHttpSink extends BaseStructuredStreamingOutput{
  var config: Config = ConfigFactory.empty()

  var options = new collection.mutable.HashMap[String, String]
  override def open(partitionId: Long, epochId: Long): Boolean = ???

  override def process(row: Row): Unit = ???

  override def close(errorOrNull: Throwable): Unit = ???

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = ???

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("postUrl") && !config.getString("postUrl").trim.isEmpty &&
      config.hasPath("timestamp") && !config.getString("timestamp").trim.isEmpty &&
      config.hasPath("metric") && !config.getString("metric").trim.isEmpty match {
      case true => {
        (true, "")
      }
      case false => (false, "[postUrl] and [timestamp] and [metric] must not be null")
    }
  }
}
