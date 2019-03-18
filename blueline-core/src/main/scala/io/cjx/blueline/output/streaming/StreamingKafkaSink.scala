package io.cjx.blueline.output.streaming
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseOutput
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.JavaConversions._
class StreamingKafkaSink extends BaseOutput{
  var kafkaSink: Option[Broadcast[KafkaSink]] = None
  val producerPrefix = "producer"
  var config: Config = ConfigFactory.empty()
  override def process(df: Dataset[Row]): Unit = {
    val dataSet = df.toJSON
    dataSet.foreach { row =>
      kafkaSink.foreach { ks =>
        ks.value.send(config.getString("topic"), row)
      }
    }
  }
  override def setConfig(config: Config): Unit = this.config=config
  override def getConfig(): Config = this.config
  override def checkConfig(): (Boolean, String) = {
    val producerConfig = config.getConfig(producerPrefix)

    config.hasPath("topic") && producerConfig.hasPath("bootstrap.servers") match {
      case true => (true, "")
      case false => (false, "please specify [topic] and [producer.bootstrap.servers]")
    }
  }
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "serializer" -> "json",
        producerPrefix + ".key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + ".value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    )

    config = config.withFallback(defaultConfig)

    val props = new Properties()
    config
      .getConfig(producerPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        props.put(key, value)
      })

    println("[INFO] Kafka Output properties: ")
    props.foreach(entry => {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    })

    kafkaSink = Some(spark.sparkContext.broadcast(KafkaSink(props)))
  }
}
class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer = createProducer()
  def send(topic: String, value: String): Unit =
    producer.send(new ProducerRecord(topic, value))
}
object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}
