package io.cjx.blueline.output.structuredstreaming

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutput
import io.cjx.blueline.utils.KafkaProducerUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
class StructuredKafkaSink extends BaseStructuredStreamingOutput{
  var config = ConfigFactory.empty()
  val producerPrefix = "producer"
  val outConfPrefix = "output.option"
  var options = new collection.mutable.HashMap[String, String]
  var kafkaSink: Broadcast[KafkaProducerUtil] = _
  var topic: String = _
  override def open(partitionId: Long, epochId: Long): Boolean = {true}

  override def process(row: Row): Unit = {
    val json = new JSONObject()
    row.schema.fieldNames
      .foreach(field => json.put(field, row.getAs(field)))
    kafkaSink.value.send(topic, json.toJSONString)
  }

  override def close(errorOrNull: Throwable): Unit = {}

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    var writer = df.writeStream
      .foreach(this)
      .options(options)
    writer = StructuredUtils.setCheckpointLocation(writer, config)
    StructuredUtils.writeWithTrigger(config, writer)
  }

  override def setConfig(config: Config): Unit = {this.config=config}

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    val producerConfig = config.getConfig(producerPrefix)

    config.hasPath("topic") && producerConfig.hasPath("bootstrap.servers") match {
      case true => (true, "")
      case false => (false, "please specify [topic] and [producer.bootstrap.servers]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    super.prepare(spark)
    topic = config.getString("topic")
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "streaming_output_mode" -> "Append",
        "trigger_type" -> "default",
        producerPrefix + ".retries" -> 2,
        producerPrefix + ".acks" -> 1,
        producerPrefix + ".buffer.memory" -> 33554432,
        producerPrefix + ".key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + ".value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    )
    config.withFallback(defaultConfig)
    val props = new Properties()
    config
      .getConfig(producerPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        props.put(key, value)
      })

    println("[INFO] Kafka Output properties: ")
    props.foreach(entry => {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    })

    kafkaSink = spark.sparkContext.broadcast(KafkaProducerUtil(props))

    config.hasPath(outConfPrefix) match {
      case true => {
        config
          .getConfig(outConfPrefix)
          .entrySet()
          .foreach(entry => {
            val key = entry.getKey
            val value = String.valueOf(entry.getValue.unwrapped())
            options.put(key, value)
          })
      }
      case false => {}
    }
  }
}
