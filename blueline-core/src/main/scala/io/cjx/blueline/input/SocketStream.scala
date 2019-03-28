package io.cjx.blueline.input

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStreamingInput
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._
class SocketStream extends BaseStreamingInput[String]{
  var config: Config = ConfigFactory.empty()
  override def rdd2dataset(spark: SparkSession, rdd: RDD[String]): Dataset[Row] = {
    val rowsRDD = rdd.map(element => {
      RowFactory.create(element)
    })

    val schema = StructType(Array(StructField("raw_message", DataTypes.StringType)))
    spark.createDataFrame(rowsRDD, schema)
  }

  override def getDStream(ssc: StreamingContext): DStream[String] = {
    ssc.socketTextStream(config.getString("host"), config.getInt("port"))
  }

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) =  (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "host" -> "localhost",
        "port" -> 9999
      ))
    config = config.withFallback(defaultConfig)
  }
}
