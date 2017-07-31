package com.esc.streaming.core

import com.esc.streaming.kafka.KafkaManager
import com.esc.streaming.message.ParseRegulation
import com.esc.streaming.monitor.{Eagle, InfoCenter}
import com.esc.streaming.util.TimeUtil
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by pc-admin on 2017/7/6.
  */
class StreamingStart(ssc:StreamingContext, dataSet:Set[(KafkaManager, InputDStream[(String,String)],String)])extends Runnable{
  var sqlContext:SQLContext = _
  override def run(): Unit = {

//          ssc.checkpoint("/spark/checkpoint")
      //    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop-common-2.2.0-bin-master")
      //    ssc.checkpoint("D:\\bea\\checkpoint")



      //    val servers = "121.201.78.37:9092,121.201.78.36:9092,121.201.78.11:9092"
      //    val kafkaSource = new KafkaSource(Set(topicsSet), servers, ssc)
      //    val kafkaLines = kafkaSource.getDStream



      //开启监控线程，监控全局变量的变化

      //    val rules = new ActualRule(topicsSet).getCountRule

      //使用测试socket
//      val lines = ssc.socketTextStream("192.168.254.128", 9933, StorageLevel.MEMORY_AND_DISK_SER)//for test


      //主要的业务逻辑代码
      dataSet.foreach{case (km,lines,topic) => {
        if(sqlContext eq null){
          val sc = lines.context.sparkContext
          sqlContext = new SQLContext(sc)
        }
        Tasks.run(km,lines,topic,sqlContext)
      }}


      InfoCenter.startTime = TimeUtil.getNow
      ssc.start()
      ssc.awaitTermination()
    }
}
