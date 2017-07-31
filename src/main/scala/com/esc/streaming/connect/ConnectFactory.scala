package com.esc.streaming.connect

import java.util.concurrent.locks.ReentrantLock

import com.esc.streaming.util.TimeUtil

/**
  * Created by pc-admin on 2017/7/4.
  */
object ConnectFactory {
  val lock = new ReentrantLock

  val conList = List("redis","mysql")

  var connectors = (for (name <- conList) yield (name,getConnectInstance(name))).toMap
  var passtime = (for (name <- conList) yield (name,TimeUtil.getMill)).toMap

    def getConnectInstance(name:String):Connector = {
      name match {
        case "redis" => new RedisConnect().init
        case "mysql" => new MysqlConnect().init
      }
    }

  def getConnector(name:String) = {
    try {
      lock.lock()
      if((TimeUtil.getMill-passtime.get(name).get)<60000*10)
      connectors.get(name).get
      else {
        connectors.get(name).get.close
        val connect = getConnectInstance(name)
        connectors += (name -> connect)
        passtime += (name -> TimeUtil.getMill)
        connect
      }
    }catch {
      case ex:Exception => "caodan"
    }finally {
      lock.unlock()
    }

  }

}
