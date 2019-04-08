package io.cjx.blueline.output.structuredstreaming

import java.sql.{Connection, PreparedStatement, Statement, Timestamp}

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingOutput
import io.cjx.blueline.utils.{DBMangerPool, HttpCallBack}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.Try
class StructuredMysqlOutput extends BaseStructuredStreamingOutput{
  var config: Config = ConfigFactory.empty()
  var MysqlDBMangerPool:DBMangerPool=_
  var DBConnection: Connection =_
  //var p: PreparedStatement= _
  private val logger: Logger = LoggerFactory.getLogger(classOf[StructuredMysqlOutput])
  override def open(partitionId: Long, epochId: Long): Boolean = {
    logger.info("===== db pool get connect")

    DBConnection=MysqlDBMangerPool.getconnection()
    true
  }

  override def process(value: Row): Unit = {
    val StatementDB = DBConnection.prepareStatement(config.getString("prepareStatement"))
    val fieldNames = value.schema.fieldNames
    for (i <- 0 until fieldNames.size) {
      value.get(i) match {
        case v: Int => StatementDB.setInt(i + 1, v)
        case v: Long => StatementDB.setLong(i + 1, v)
        case v: Float => StatementDB.setFloat(i + 1, v)
        case v: Double => StatementDB.setDouble(i + 1, v)
        case v: String => StatementDB.setString(i + 1, v)
        case v: Timestamp => StatementDB.setTimestamp(i + 1, v)
      }
    }
    //var index_1= 1
    //fieldNames.foreach(f = name => {
      //val v = value.get(value.fieldIndex(name))
      //StatementDB.setString(index_1, v.toString)
      //index_1 = index_1 + 1
    //})
    StatementDB.execute()

    logger.info("===== db pool addBatch date")
  }

  override def close(errorOrNull: Throwable): Unit = {
    this.DBConnection.close()

    logger.info("===== db pool close connect")
  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    val triggerMode = config.getString("triggerMode")

    var writer = df.coalesce(2).writeStream
      .outputMode(config.getString("outputMode"))
      .foreach(this)

    writer = StructuredUtils.setCheckpointLocation(writer, config)

    triggerMode match {
      case "default" => writer
      case "ProcessingTime" => writer.trigger(Trigger.ProcessingTime(config.getString("interval")))
      case "OneTime" => writer.trigger(Trigger.Once())
      case "Continuous" => writer.trigger(Trigger.Continuous(config.getString("interval")))
    }
  }

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("mysqlurl") && config.hasPath("outputMode")&&config.hasPath("password")&&config.hasPath("user")&&
      config.hasPath("prepareStatement") match {
      case false => (false, "配置项不正确")
      case true => {
        (true, "")
      }
  }}

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "outputMode" -> "Append",
        "triggerMode" -> "default",
        "maxpoolsize" -> 20,
        "minpoolsize" -> 1
      )
    )
    config = config.withFallback(defaultConfig)
    logger.info("===== start init db pool")
    MysqlDBMangerPool=new DBMangerPool(config.getString("user"),
      config.getString("password"),config.getString("mysqlurl"),config.getInt("maxpoolsize"),config.getInt("minpoolsize"))
    logger.info("===== end init db pool")
    //DataCenter.DBMangerPool_ =new DBMangerPool(config.getString("user"),
      //config.getString("password"),config.getString("mysqlurl"),config.getInt("maxpoolsize"),config.getInt("minpoolsize"))
  }
}
