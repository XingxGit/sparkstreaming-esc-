package com.esc.streaming.regulation

/**
  * Created by zhangxing on 2017/6/22.
  */
case class TimeCountRule(title:String,timeCountVariable:Set[String],time:Int) {
      def getTimeCountVariable = {
        this.timeCountVariable
      }

      def getTime = {
        this.time
      }
}

