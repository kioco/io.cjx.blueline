package io.cjx.blueline.input

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingInput
import io.cjx.blueline.core.RowConstant
import io.cjx.blueline.utils.SparkSturctTypeUtil
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
class StructuredKafkaStream extends BaseStructuredStreamingInput{
  var config: Config = ConfigFactory.empty()
  val consumerPrefix = "consumer"
  var schema = new StructType()
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val topics = config.getString("topics")
    val consumerConfig = config.getConfig(consumerPrefix)
    val kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })

    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
    var dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("consumer.bootstrap.servers"))
      .option("subscribe", config.getString("topics"))
      .options(kafkaParams)
      .load()
    if (schema.size > 0) {
      var tmpDf = dataFrame.withColumn(RowConstant.TMP, from_json(col("value").cast(DataTypes.StringType), schema))
      schema.map { field =>
        tmpDf = tmpDf.withColumn(field.name, col(RowConstant.TMP)(field.name))
      }
      dataFrame = tmpDf.drop(RowConstant.TMP)
      if (config.hasPath("table_name")) {
        dataFrame.createOrReplaceTempView(config.getString("table_name"))
      }
    }
    dataFrame
  }

  override def setConfig(config: Config): Unit = {
    this.config=config
  }

  override def getConfig(): Config = {this.config}

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("consumer.bootstrap.servers") && config.hasPath("topics") match {
      case true => (true, "")
      case false => (false, "please specify [consumer.bootstrap.servers] and [topics] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit ={
    config.hasPath("schema") match {
      case true => {
        val schemaJson = JSON.parseObject(config.getString("schema"))
        schema = SparkSturctTypeUtil.getStructType(schema, schemaJson)
      }
      case false => {}
    }
  }
}
