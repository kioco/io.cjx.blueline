package io.cjx.blueline.input

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.typesafe.config.{Config, ConfigFactory}
import io.cjx.blueline.apis.BaseStructuredStreamingInput
import io.cjx.blueline.config.TypesafeConfigUtils
import io.cjx.blueline.core.RowConstant
import io.cjx.blueline.utils.SparkSturctTypeUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
class StructuredKafkaStream_v2 extends BaseStructuredStreamingInput{
  var config: Config = ConfigFactory.empty()
  val consumerPrefix = "consumer."
  var schema = new StructType()
  val poll = 100.toLong
  var topics: String = _
  var consumer: KafkaConsumer[String,String] = _
  var kafkaParams: Map[String,String] = _
  val offsetMeta = new util.HashMap[String,util.HashMap[String,Long]]()
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    var dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("consumer.bootstrap.servers"))
      .option("subscribe", topics)
      .options(kafkaParams)
      .load()
    if (schema.size > 0) {
      var tmpDf = dataFrame.withColumn(RowConstant.TMP, from_json(col("value").cast(DataTypes.StringType), schema))
      schema.map { field =>
        tmpDf = tmpDf.withColumn(field.name, col(RowConstant.TMP)(field.name))
      }
      dataFrame = tmpDf.drop(RowConstant.TMP)
      if (config.hasPath("table_name")) {
        dataFrame.createOrReplaceTempView(config.getString("table_name"))
      }
    }
    dataFrame
  }

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = this.config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("consumer.bootstrap.servers") && config.hasPath("topics") &&
      config.hasPath("consumer.group.id") match {
      case true => (true, "")
      case false => (false, "please specify [consumer.bootstrap.servers] and [topics] and [consumer.group.id] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    topics = config.getString("topics")

    config.hasPath("schema") match {
      case true => {
        val schemaJson = JSON.parseObject(config.getString("schema"))
        schema = SparkSturctTypeUtil.getStructType(schema, schemaJson)
      }
      case false => {}
    }
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {

      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        //这个是kafkaInput的正则匹配
        val pattern = Pattern.compile(".+\\[Subscribe\\[(.+)\\]\\]");
        val sources = event.progress.sources
        sources.foreach(source =>{
          val matcher = pattern.matcher(source.description)
          if (matcher.find() && topics.equals(matcher.group(1))){
            //每个input负责监控自己的topic
            val endOffset = JSON.parseObject(source.endOffset)
            endOffset
              .keySet()
              .foreach(topic => {
                val partitionToOffset = endOffset.getJSONObject(topic)
                partitionToOffset
                  .keySet()
                  .foreach(partition=>{
                    val offset = partitionToOffset.getLong(partition)
                    val topicPartition = new TopicPartition(topic,Integer.parseInt(partition))
                    val value = consumer.poll(poll)
                    consumer.seek(topicPartition,offset)
                  })
              })
          }
        })
        consumer.commitSync()
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        consumer.close()
      }

      })
    val consumerConfig = TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
    kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })

    val topicList = initConsumer()

    config.hasPath("offset.location") match {
      case true => config.getString("offset.location").equals("broker") match {
        case true => {
          setOffsetMeta(topicList)
          kafkaParams += ("startingOffsets" -> JSON.toJSONString(offsetMeta,SerializerFeature.WriteMapNullValue))
        }
      }
    }
    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

  }
  private def initConsumer():  util.ArrayList[String]={
    val props = new Properties()
    props.put("bootstrap.servers",config.getString("consumer.bootstrap.servers"))
    props.put("group.id", config.getString("consumer.group.id"))
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumer = new KafkaConsumer[String, String](props)
    val topicList = new util.ArrayList[String]()
    topics.split(",").foreach(topicList.add(_))
    consumer.subscribe(topicList)
    topicList
  }
  private def setOffsetMeta(topicList: util.ArrayList[String]): Unit={
    topicList.foreach(topic =>{
      val partition2offset = new util.HashMap[String,Long]()
      val topicInfo = consumer.partitionsFor(topic)
      topicInfo.foreach(info => {
        val topicPartition = new TopicPartition(topic,info.partition())
        val metadata = consumer.committed(topicPartition)
        partition2offset.put(String.valueOf(info.partition()),metadata.offset())
      })
      offsetMeta.put(topic,partition2offset)
    })
  }

}
