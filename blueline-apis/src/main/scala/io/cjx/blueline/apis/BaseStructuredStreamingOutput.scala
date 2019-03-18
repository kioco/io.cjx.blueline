package io.cjx.blueline.apis

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, ForeachWriter, Row}

trait BaseStructuredStreamingOutput extends ForeachWriter[Row] with BaseStructuredStreamingOutputIntra{

  def open(partitionId: Long, epochId: Long): Boolean

  def process(row: Row): Unit


  def close(errorOrNull: Throwable): Unit


  def process(df: Dataset[Row]): DataStreamWriter[Row]
}
