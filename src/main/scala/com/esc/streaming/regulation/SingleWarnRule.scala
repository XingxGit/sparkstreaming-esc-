package com.esc.streaming.regulation

/**
  * Created by zhangxing on 2017/6/22.
  */
case class SingleWarnRule(topic:String, countVariable:Set[String]) {

      def getCountVariable = {
        this.countVariable
      }


}
