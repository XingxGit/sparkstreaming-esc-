package com.esc.streaming.core

import com.esc.streaming.kafka.KafkaSource
import com.esc.streaming.message.ParseRegulation
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by pc-admin on 2017/7/6.
  */
class RunSparkStreaming(sc:SparkContext,recover:Boolean,topics:Set[String]){

  private var ssc:StreamingContext = _
  private var runTopic:Map[String,DStream[String]] = Map()
  private def createContext(str: String) = {
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(str)
    ssc
  }
  private def initSSC(sc:SparkContext) = {
    val ckpath = "/spark/checkpoint"
//    if(recover==true){
//      ssc = StreamingContext.getOrCreate(ckpath,
//        () => createContext(ckpath))

//    }else{
      ssc = new StreamingContext(sc, Seconds(2))
      ssc.checkpoint(ckpath)

//    }

  }

  def run(): Unit = {
    initSSC(sc)
    if(recover==true){
      val listLines = topics.map(topic =>{
        val source = new KafkaSource(ssc,Set(topic)).getDStream()
        (source._1,source._2,topic)
        })
      new Thread(new StreamingStart(ssc,listLines)).start()
    }
    if(recover==false){
//      val lines = ssc.socketTextStream("121.201.78.13", 9933, StorageLevel.MEMORY_AND_DISK_SER).repartition(2)
//      new Thread(new StreamingStart(ssc,lines,"esc")).start()
    }

//    Thread.sleep(30000)
//    try{
//      println("try to stop ssc!!!")
//      ssc.stop(false,true)
//    }catch {
//      case e:Exception => e.printStackTrace()
//    }
//
//    val ssc2 = new StreamingContext(sc, Seconds(2))
//    new Thread(new SscStart(ssc2,bg,"fred")).start()
//    ssc.start()
//    ssc.awaitTermination()
  }

  def stop(topic:String) = {
    val lines = runTopic.get(topic).get

  }

}
