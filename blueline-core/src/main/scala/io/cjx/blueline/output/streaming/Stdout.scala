package io.cjx.blueline.output.streaming

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row}

class StreamingStdout extends BaseOutput{
  var config: Config = ConfigFactory.empty()
  override def process(df: Dataset[Row]): Unit = {
    val limit = config.getInt("limit")

    config.getString("serializer") match {
      case "plain" => {
        if (limit == -1) {
          df.show(Int.MaxValue, false)
        } else if (limit > 0) {
          df.show(limit, false)
        }
      }
      case "json" => {
        if (limit == -1) {
          df.toJSON.take(Int.MaxValue).foreach(s => println(s))

        } else if (limit > 0) {
          df.toJSON.take(limit).foreach(s => println(s))
        }
      }
    }
  }

  override def setConfig(config: Config): Unit = {this.config=config}

  override def getConfig(): Config = {this.config}

  override def checkConfig(): (Boolean, String) = {
    !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1) match {
      case true => (true, "")
      case false => (false, "please specify [limit] as Number[-1, " + Int.MaxValue + "]")
    }
  }
}
