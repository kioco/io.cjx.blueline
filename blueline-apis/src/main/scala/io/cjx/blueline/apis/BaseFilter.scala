package io.cjx.blueline.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}

abstract class BaseFilter extends Plugin{
  def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row]


  def getUdfList(): List[(String, UserDefinedFunction)] = List.empty


  def getUdafList(): List[(String, UserDefinedAggregateFunction)] = List.empty
}
