package com.esc.streaming.monitor

/**
  * Created by pc-admin on 2017/6/29.
  */
object EventFactory {
  var eventMap:Map[String,EventCenter] = Map()
  def register(eagleName: String): EventCenter ={
      val eventCenter = new EventCenter
      eventMap += (eagleName -> eventCenter)
      eventCenter
  }
}
