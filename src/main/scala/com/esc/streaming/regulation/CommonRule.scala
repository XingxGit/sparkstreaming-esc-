package com.esc.streaming.regulation

/**
  * Created by zhangxing on 2017/6/22.
  */
case class CommonRule(topic:String, regexs:Map[String,String]) {

  def getTopic() = {
    this.topic
  }

  def getRules() = {
    this.regexs
  }

}


