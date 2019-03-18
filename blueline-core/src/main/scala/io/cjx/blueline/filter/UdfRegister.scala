package io.cjx.blueline.filter

import scala.collection.JavaConverters._
import java.util.ServiceLoader

import io.cjx.blueline.apis.BaseFilter
import org.apache.spark.sql.SparkSession

object UdfRegister {
  def findAndRegisterUdfs(spark: SparkSession): Unit = {

    println("find and register UDFs & UDAFs")

    var udfCount = 0
    var udafCount = 0
    val services = (ServiceLoader load classOf[BaseFilter]).asScala
    services.foreach(f => {

      f.getUdfList()
        .foreach(udf => {
          val (udfName, udfImpl) = udf
          spark.udf.register(udfName, udfImpl)
          udfCount += 1
        })

      f.getUdafList()
        .foreach(udaf => {
          val (udafName, udafImpl) = udaf
          spark.udf.register(udafName, udafImpl)
          udafCount += 1
        })
    })

    println("found and registered UDFs count[" + udfCount + "], UDAFs count[" + udafCount + "]")
  }
}
