package io.cjx.blueline.apis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

abstract class BaseStreamingInput[T] extends Plugin {


  def beforeOutput: Unit = {}


  def afterOutput: Unit = {}


  def rdd2dataset(spark: SparkSession, rdd: RDD[T]): Dataset[Row]

  def start(spark: SparkSession, ssc: StreamingContext, handler: Dataset[Row] => Unit): Unit = {

    getDStream(ssc).foreachRDD(rdd => {
      val dataset = rdd2dataset(spark, rdd)
      handler(dataset)
    })
  }


  def getDStream(ssc: StreamingContext): DStream[T]
}
