package io.cjx.blueline.apis

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

trait BaseStructuredStreamingOutputIntra extends Plugin{
  def process(df: Dataset[Row]): DataStreamWriter[Row]
}
