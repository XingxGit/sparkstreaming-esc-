package com.esc.streaming.core

import com.esc.streaming.config.ActualRule
import com.esc.streaming.monitor.{Eagle, InfoCenter}
import com.esc.streaming.netty.NettyServer
import com.esc.streaming.util.TimeUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangxing on 2017/6/8.
  */
//spark streaming 主程序入口
object Process {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("kafkaProcess")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "3")
//    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    val sc = new SparkContext(sparkConf)
    GlobalValue.sc = sc
    GlobalValue.init//初始化全局变量
    val bg = GlobalValue.values.keySet
    GlobalValue.bc = sc.broadcast(GlobalValue.regs)
    GlobalValue.rss = new RunSparkStreaming(sc,true,bg)
//    new Thread(new NettyServer(sc)).start()
    GlobalValue.rss.run()
    //初始化streaming context，时间为2秒计算一次
//    val ssc = new StreamingContext(sc, Seconds(2))
////    ssc.checkpoint("/spark/checkpoint")
//    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop-common-2.2.0-bin-master")
//    ssc.checkpoint("D:\\bea\\checkpoint")
//    val topicsSet = "fred"
//
//
//
////    val servers = "121.201.78.37:9092,121.201.78.36:9092,121.201.78.11:9092"
////    val kafkaSource = new KafkaSource(Set(topicsSet), servers, ssc)
////    val kafkaLines = kafkaSource.getDStream
//
//
//
//    //开启监控线程，监控全局变量的变化
//    new Thread(new Eagle("transactionMonitor",bg,ssc)).start()
////    val rules = new ActualRule(topicsSet).getCountRule
//
//    //使用测试socket
//    val lines = ssc.socketTextStream("192.168.254.128", 9933, StorageLevel.MEMORY_AND_DISK_SER)//for test
//
//
//    //主要的业务逻辑代码
//    Tasks.run(topicsSet,lines,bg)
//
//    InfoCenter.startTime = TimeUtil.getNow
//    ssc.start()
//    ssc.awaitTermination()
  }

}
