package com.esc.streaming.monitor

import com.esc.streaming.constant.WarnLevel.WarnLevel

/**
  * Created by pc-admin on 2017/6/29.
  */
class EventCenter {

  //记录具体事件类
  case class event(content:String,time:String)
  //事件总线
  private[monitor] var eventBus:Map[String,Map[String,event]] = Map()
  //具体处理事件的代码
  def receiveEvent(level:WarnLevel,eventID:String,content:String,time:String): Unit ={
      if(eventBus.contains(level.toString)){
        var eventMap = eventBus.get(level.toString).get
        if(eventMap.contains(eventID)){
          val innerevent = eventMap.get(eventID).get
          if(time.compareTo(innerevent.time)>0){
              eventMap += (eventID -> event(content,time))
          }
        }

      }else{
        eventBus += (level.toString -> Map(eventID -> event(content,time)))
      }
  }

}
