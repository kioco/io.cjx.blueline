package io.cjx.blueline.input

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStreamingInput
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._
class StreamingKafkaStream extends BaseStreamingInput[(String, String)]{
  var config: Config = ConfigFactory.empty()
  val consumerPrefix = "consumer"
  var inputDStream: InputDStream[ConsumerRecord[String, String]] = _

  var offsetRanges = Array[OffsetRange]()
  override def rdd2dataset(spark: SparkSession, rdd: RDD[(String, String)]): Dataset[Row] = {
    val rowsRDD = rdd.map(element => {
      element match {
        case (topic, message) => {
          RowFactory.create(topic, message)
        }
      }
    })

    val schema = StructType(
      Array(StructField("topic", DataTypes.StringType), StructField("raw_message", DataTypes.StringType)))
    spark.createDataFrame(rowsRDD, schema)
  }
  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {
    val consumerConfig = config.getConfig(consumerPrefix)
    val kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })
    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

    val topics = config.getString("topics").split(",").toSet
    inputDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams))

    inputDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(record => {
        val topic = record.topic()
        val value = record.value()
        (topic, value)
      })
    }
  }
  override def setConfig(config: Config): Unit = this.config=config
  override def getConfig(): Config = this.config
  override def checkConfig(): (Boolean, String) = {
    config.hasPath("topics") match {
      case true => {
        val consumerConfig = config.getConfig(consumerPrefix)
        consumerConfig.hasPath("group.id") &&
          !consumerConfig.getString("group.id").trim.isEmpty match {
          case true => (true, "")
          case false =>
            (false, "please specify [consumer.group.id] as non-empty string")
        }
      }
      case false => (false, "please specify [topics] as non-empty string, multiple topics separated by \",\"")
    }
  }
  override def afterOutput: Unit = {
    inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    for (offsets <- offsetRanges) {
      val fromOffset = offsets.fromOffset
      val untilOffset = offsets.untilOffset
      if (untilOffset != fromOffset) {
        println(
          s"complete consume topic: ${offsets.topic} partition: ${offsets.partition} from ${fromOffset} until ${untilOffset}")
      }
    }
  }
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        consumerPrefix + ".key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        consumerPrefix + ".value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        consumerPrefix + ".enable.auto.commit" -> false
      )
    )

    config = config.withFallback(defaultConfig)
  }
}
