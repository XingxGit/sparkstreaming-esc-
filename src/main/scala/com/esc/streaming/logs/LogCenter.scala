package com.esc.streaming.logs

/**
  * Created by pc-admin on 2017/6/27.
  */
object LogCenter {


  def getLogger(clazz:Class[_]):CustomLog = {
      new CustomLog(clazz.getName)
  }


}
