package io.cjx.blueline.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.internal.Logging
class MysqlSource (url: String, user: String, pwd: String,prepareStatement:String,conn: Connection) extends ForeachWriter[Row] with Logging with Serializable{
  var p: PreparedStatement= _
  override def open(partitionId: Long, epochId: Long): Boolean = {
    p = conn.prepareStatement(prepareStatement)
    conn.setAutoCommit(false)
    logInfo("===== get connect in pool")
    true
  }

  override def process(value: Row): Unit = {
    val fieldNames = value.schema.fieldNames
    var index_1= 1
    fieldNames.foreach(f = name => {
      val v = value.get(value.fieldIndex(name))
      p.setString(index_1, v.toString)
      index_1 = index_1 + 1
    })
    p.addBatch()
    logInfo("===== insert into table")
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (errorOrNull == null) {
      p.executeBatch()
      conn.commit()
    } else {
      conn.rollback()
    }
    p.clearParameters()
    conn.close()
    logInfo("===== close db in pool")
  }


}
