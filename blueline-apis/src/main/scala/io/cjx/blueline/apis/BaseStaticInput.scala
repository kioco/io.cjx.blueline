package io.cjx.blueline.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseStaticInput extends Plugin {


  def getDataset(spark: SparkSession): Dataset[Row]
}
