package io.cjx.blueline.apis

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait Plugin extends Serializable with Logging {

  def setConfig(config: Config): Unit


  def getConfig(): Config


  def checkConfig(): (Boolean, String)


  def name: String = this.getClass.getName


  def prepare(spark: SparkSession): Unit = {}
}
