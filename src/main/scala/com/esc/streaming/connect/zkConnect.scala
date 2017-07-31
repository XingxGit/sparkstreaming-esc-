package com.esc.streaming.connect

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by zhangxing on 2017/6/20.
  */
class zkConnect extends Connector{
  var zk:ZooKeeper = _
  private val TIME_OUT = 5000
  private val countdown = new CountDownLatch(1)
  var host:String = _
  var stat = new Stat()
  override def init: zkConnect = {
    host  = "121.201.78.37:2181,121.201.78.36:2181,121.201.78.11:2181"
    this
  }

  override def connect: Any = {
    zk = new ZooKeeper(host,TIME_OUT,new zkWather)
    countdown.await()
  }

  def getChildren(path:String) = {
    zk.getChildren(path,false)
  }

  def createPath(path:String,data:String) = {
    zk.create(path,data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT)
  }

  def getData(path:String):String = {
    zk.getData(path,false,stat).toString
  }

  def setData(path:String,data:String) = {
    zk.setData(path,data.getBytes(),-1)
  }

  class zkWather extends Watcher{
    override def process(watchedEvent: WatchedEvent): Unit = {
      if(watchedEvent.getState == KeeperState.SyncConnected){
        countdown.countDown()
      }
    }
  }

  override def close: Unit = {
    if(zk!=null)zk.close()
  }
}

