package com.esc.streaming.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by pc-admin on 2017/6/26.
  */
object TimeUtil {
    def getNow:String = {
      val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      dataFormat.format(new Date())
    }

    def getMill:Long = {
      System.currentTimeMillis()
    }
}
