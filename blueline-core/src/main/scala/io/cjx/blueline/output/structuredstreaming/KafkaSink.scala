package io.cjx.blueline.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutputIntra
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
class KafkaSink extends BaseStructuredStreamingOutputIntra{
  var config: Config = ConfigFactory.empty()
  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    val triggerMode = config.getString("triggerMode")
    var writer = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("producer.bootstrap.servers"))
      .option("topic", config.getString("topic"))
      .outputMode(config.getString("outputMode"))

    writer = StructuredUtils.setCheckpointLocation(writer, config)

    triggerMode match {
      case "default" => writer
      case "ProcessingTime" => writer.trigger(Trigger.ProcessingTime(config.getString("interval")))
      case "OneTime" => writer.trigger(Trigger.Once())
      case "Continuous" => writer.trigger(Trigger.Continuous(config.getString("interval")))
    }
  }

  override def setConfig(config: Config): Unit   = this.config = config

  override def getConfig(): Config = {this.config}

  override def checkConfig(): (Boolean, String) = {
    !config.hasPath("producer.bootstrap.servers") || !config.hasPath("topic") match {
      case true => (false, "please specify [producer.bootstrap.servers] and [topic]")
      case false => {
        StructuredUtils.checkTriggerMode(config) match {
          case true => (true, "")
          case false => (false, "please specify [interval] when [triggerMode] is ProcessingTime or Continuous")
        }
      }
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "outputMode" -> "Append",
        "triggerMode" -> "default"
      )
    )
    config = config.withFallback(defaultConfig)
  }
}
