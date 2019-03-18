package io.cjx.blueline.output.streaming
import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.JavaConversions._
class StreamingJdbcSink extends BaseOutput{
  var firstProcess = true

  var config: Config = ConfigFactory.empty()
  override def process(df: Dataset[Row]): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", config.getString("driver"))
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))

    val saveMode = config.getString("save_mode")

    if (firstProcess) {
      df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop)
      firstProcess = false
    } else if (saveMode == "overwrite") {
      // actually user only want the first time overwrite in streaming(generating multiple dataframe)
      df.write.mode(SaveMode.Append).jdbc(config.getString("url"), config.getString("table"), prop)
    } else {
      df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop)
    }
  }
  override def setConfig(config: Config): Unit = this.config=config
  override def getConfig(): Config = this.config
  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("driver", "url", "table", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length == 0) {

      val saveModeAllowedValues = List("overwrite", "append", "ignore", "error");

      if (!config.hasPath("save_mode") || saveModeAllowedValues.contains(config.getString("save_mode"))) {
        (true, "")
      } else {
        (false, "wrong value of [save_mode], allowed values: " + saveModeAllowedValues.mkString(", "))
      }

    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    }
  }
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "append" // allowed values: overwrite, append, ignore, error
      )
    )
    config = config.withFallback(defaultConfig)
  }
}
