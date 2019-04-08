package io.cjx.blueline.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseFilter
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
class WaterMark_v2 extends BaseFilter{
  var config: Config = ConfigFactory.empty()
  var timeField: String = _
  var waterMarkField: String = _
  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val realDf = config.hasPath("source_table_name") match {
      case true => spark.table(config.getString("source_table_name"))
      case false => df
    }
    val pattern = config.getString("time_pattern")
    val newDf = config.getString("time_type") match {
      case "timestamp_13" => realDf.withColumn(waterMarkField, expr(s"to_timestamp(from_unixtime($timeField /1000))"))
      case "timestamp_10" => realDf.withColumn(waterMarkField, expr(s"to_timestamp(from_unixtime($timeField))"))
      case "string" => realDf.withColumn(waterMarkField, expr(s"to_timestamp($timeField,'$pattern')"))
    }
    val waterMarkDf = newDf.withWatermark(waterMarkField,config.getString("delay_threshold"))
    if (config.hasPath("result_table_name")){
      waterMarkDf.createOrReplaceTempView(config.getString("result_table_name"))
    }
    waterMarkDf
  }

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("time_field") && config.hasPath("delay_threshold")&& config.hasPath("watermark_field") &&
      config.hasPath("time_type") match {
      case true => (true, "")
      case false => (false, "please specify [time_field] and [delay_threshold] and [watermark_field] and [time_type]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    timeField = config.getString("time_field")
    waterMarkField = config.getString("watermark_field")
    val defaultConfig = ConfigFactory.parseMap(
      Map("time_pattern" -> "yyyy-MM-dd HH:mm:ss")
    )
    config = config.withFallback(defaultConfig)
  }
}
