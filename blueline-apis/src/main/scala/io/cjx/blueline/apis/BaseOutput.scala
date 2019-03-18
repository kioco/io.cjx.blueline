package io.cjx.blueline.apis

import org.apache.spark.sql.{Dataset, Row}

abstract class BaseOutput extends Plugin {

  def process(df: Dataset[Row])
}
