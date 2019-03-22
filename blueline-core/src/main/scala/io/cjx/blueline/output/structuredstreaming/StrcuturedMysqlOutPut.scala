package io.cjx.blueline.output.structuredstreaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutput
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
class StrcuturedMysqlOutPut extends BaseStructuredStreamingOutput{
  var config: Config = ConfigFactory.empty()
  var conn: Connection = _
  var p: PreparedStatement= _
  override def open(partitionId: Long, epochId: Long): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(config.getString("mysqlurl"),config.getString("user"),config.getString("password"))
    p = conn.prepareStatement(config.getString("prepareStatement"))
    true
  }
  override def process(row: Row): Unit = {

    val fieldNames = row.schema.fieldNames
    var index_1= 1
    fieldNames.foreach(f = name => {
      val v = row.get(row.fieldIndex(name))
      p.setString(index_1, v.toString)
      index_1 = index_1 + 1
    })
    p.execute()
  }

  override def close(errorOrNull: Throwable): Unit = {
    if(conn!=null){
      conn.close()
    }

  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    var writer = df.writeStream
      .outputMode(config.getString("OutPutMode"))
      .foreach(this)
    writer = StructuredUtils.setCheckpointLocation(writer, config)
    writer
  }

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("mysqlurl") && config.hasPath("checkpointLocation")&&config.hasPath("password")&&config.hasPath("user")&&
      config.hasPath("prepareStatement") match {
      case false => (false, config.getString("mysqlurl"))
      case true => {
        (true, "")
      }
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "OutPutMode" -> "Append"
      )
    )
    config = config.withFallback(defaultConfig)
  }
}
