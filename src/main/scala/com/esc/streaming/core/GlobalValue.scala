package com.esc.streaming.core

import com.esc.streaming.config.ActualRule
import com.esc.streaming.message.{Attribute, ParseRegulation}
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.broadcast.Broadcast

/**
  * Created by pc-admin on 2017/6/26.
  */
object GlobalValue {
  var regs:List[ParseRegulation] = _
  var sc:SparkContext = _
  var values:Map[String,List[ParseRegulation]] = Map()
  var rss:RunSparkStreaming = _
  @volatile  var bc:Broadcast[List[ParseRegulation]] = _
  def init = {
      values += ("he" -> List(ParseRegulation("he","es","(?<=name=)[a-zA-Z]+|(?<=age=)[0-9]+",10l,"select * from es where name='w'",List(Attribute("name","string",0),Attribute("age","int",1)))))
      values += ("hi" -> List(ParseRegulation("hi","sc","(?<=address=)[a-zA-Z]+|(?<=times=)[0-9]+",10l,"select name,age,address,times from sc,es where address='shenzhen' and name='xing'",List(Attribute("address","string",0),Attribute("times","int",1)))))
      regs = values.values.flatten.toList
  }

  def getBC()= {
      synchronized {
          sc.broadcast(regs)
        }
}

  def getCountMap(topic:String) = values.get(topic) match {
    case Some(map) => map
    case _ => None
//    {
//      values.synchronized{
//        fillValue(topic)
//        values.get(topic).getOrElse(Nil)
//      }
//
//    }
  }




}
