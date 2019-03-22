package io.cjx.blueline.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseFilter
import io.cjx.blueline.core.RowConstant
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
class Split extends BaseFilter{
  var conf: Config = ConfigFactory.empty()
  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val srcField = conf.getString("source_field")
    val keys = conf.getStringList("fields")

    // https://stackoverflow.com/a/33345698/1145750
    conf.getString("target_field") match {
      case RowConstant.ROOT => {
        val func = udf((s: String) => {
          split(s, conf.getString("delimiter"), keys.size())
        })
        var filterDf = df.withColumn(RowConstant.TMP, func(col(srcField)))
        for (i <- 0 until keys.size()) {
          filterDf = filterDf.withColumn(keys.get(i), col(RowConstant.TMP)(i))
        }
        filterDf.drop(RowConstant.TMP)
      }
      case targetField: String => {
        val func = udf((s: String) => {
          val values = split(s, conf.getString("delimiter"), keys.size)
          val kvs = (keys zip values).toMap
          kvs
        })

        df.withColumn(targetField, func(col(srcField)))
      }
    }
  }

  override def setConfig(config: Config): Unit = this.conf=config

  override def getConfig(): Config = this.conf

  override def checkConfig(): (Boolean, String) = {conf.hasPath("fields") && conf.getStringList("fields").size() > 0 match {
    case true => (true, "")
    case false => (false, "please specify [fields] as a non-empty string list")
  }}

  override def prepare(spark: SparkSession): Unit ={
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "delimiter" -> " ",
        "source_field" -> "raw_message",
        "target_field" -> RowConstant.ROOT
      )
    )

    conf = conf.withFallback(defaultConfig)
  }
  private def split(str: String, delimiter: String, fillLength: Int): Seq[String] = {
    val parts = str.split(delimiter).map(_.trim)
    val filled = (fillLength compare parts.size) match {
      case 0 => parts
      case 1 => parts ++ Array.fill[String](fillLength - parts.size)("")
      case -1 => parts.slice(0, fillLength)
    }
    filled.toSeq
  }
}
