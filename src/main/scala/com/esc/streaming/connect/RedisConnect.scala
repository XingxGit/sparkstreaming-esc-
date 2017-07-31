package com.esc.streaming.connect

import redis.clients.jedis.Jedis

/**
  * Created by pc-admin on 2017/7/3.
  */
class RedisConnect extends Connector{
  private var host = ""
  private var port = 0
  private var conn:Jedis = _
  override def init:RedisConnect = {
    host = "127.0.0.1"
    port = 6379
    this
  }

  override def connect: Jedis = {
    conn = new Jedis(host,port)
    conn
  }

  override def close: Unit = {
    if(conn!=null)
      conn.close()
  }
}


