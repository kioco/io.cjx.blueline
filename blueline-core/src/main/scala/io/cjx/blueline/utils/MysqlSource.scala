package io.cjx.blueline.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.internal.Logging
class MysqlSource (url: String, user: String, pwd: String,prepareStatement:String) extends ForeachWriter[Row]{
  var conn: Connection= _
  var p: PreparedStatement= _
  override def open(partitionId: Long, epochId: Long): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, user, pwd)
    p = this.conn.prepareStatement(prepareStatement)
    conn.setAutoCommit(false)
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
  }

  override def close(errorOrNull: Throwable): Unit = {
    if(conn != null){
      if(errorOrNull == null){
        p.executeBatch()
        conn.commit()
        p.close()

      }else{
        conn.rollback()
      }
      conn.close()
    }
  }
}
