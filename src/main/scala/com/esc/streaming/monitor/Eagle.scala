package com.esc.streaming.monitor

import com.esc.streaming.constant.WarnLevel
import com.esc.streaming.logs.LogCenter
import com.esc.streaming.util.TimeUtil
import org.apache.spark.Accumulator
import org.apache.spark.streaming.StreamingContext

/**
  * Created by pc-admin on 2017/6/28.
  */
class Eagle(name:String,globalValue:Map[String,Map[String,Accumulator[Int]]],ssc:StreamingContext) extends Runnable{
  private val log = LogCenter.getLogger(classOf[Eagle].getClass)
  var checkRule:Map[String,Map[String,Long]] = _
  var eventCenter:EventCenter = _
  def init = {
    checkRule = Map("fred" -> Map("fredCount" -> 5L))
    eventCenter = EventFactory.register(name)
    log.info("the eagel finish init...")
  }

  def makeWarn(msg:String): Unit ={
    log.warn(msg)
  }

  override def run(): Unit = {
    init
    while (true) {
      globalValue.foreach(topic =>{
        checkRule.get(topic._1) match {
          case Some(countRule:Map[String,Long]) =>{
            topic._2.foreach(counts =>{
              val countName = counts._1
//              if(countRule.contains(countName)&&(counts._2.value.compareTo(countRule.get(countName).getOrElse[Int](Int.MaxValue))>0)){
//                eventCenter.receiveEvent(WarnLevel(3),countName,"count value is:"+counts._2.value+" over the warn level",TimeUtil.getNow)
//                makeWarn(countName+" count value is:"+counts._2.value+" is over the warn level... ")
//              }else {
//                log.info(countName+"count value is:"+counts._2.value+" under limit")
//                eventCenter.receiveEvent(WarnLevel(5),countName,"count value is:"+counts._2.value+" under limit",TimeUtil.getNow)
//              }
            })
          }
          case _ =>
        }

      })
      Thread.sleep(10000)
      ssc.stop(false,true)
      Thread.sleep(5000)
      ssc.start()
      ssc.awaitTermination()
    }
  }
}
