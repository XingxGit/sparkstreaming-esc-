package com.esc.streaming.connect

import java.sql.Connection

/**
  * Created by zhangxing on 2017/6/21.
  */
class MysqlConnect extends Connector{
  private var user = ""
  private var password = ""
  private var host = ""
  private var database = ""
  private var conn_str = ""
  private var conn:Connection = _
  override def init: MysqlConnect = {
    user="root"
    password = "123456"
    host="121.201.78.13"
    database="test"
    conn_str = "jdbc:mysql://"+host +":3306/"+database+"?user="+user+"&password=" + password
    this
  }

  override def connect: Connection = {
    import java.sql.DriverManager
    Class.forName("com.mysql.jdbc.Driver").newInstance
    conn = DriverManager.getConnection(conn_str)
    conn
  }

  override def close: Unit = {
    if(conn!=null)
    conn.close()
  }
}
