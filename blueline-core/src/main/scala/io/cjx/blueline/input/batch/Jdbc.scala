package io.cjx.blueline.input.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Jdbc extends BaseStaticInput{
  var config: Config = ConfigFactory.empty()
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    spark.read
      .format("jdbc")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("driver", config.getString("driver"))
      .load()
  }

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("url", "table", "user", "password")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.isEmpty) {
      (true, "")
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }
}
