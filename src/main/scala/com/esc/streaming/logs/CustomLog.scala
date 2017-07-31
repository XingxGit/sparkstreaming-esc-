package com.esc.streaming.logs

import com.esc.streaming.util.TimeUtil

/**
  * Created by pc-admin on 2017/7/4.
  */
class CustomLog(name:String) {

  def getIdenty:String = {
    "["+name+"]"+TimeUtil.getNow
  }
  def info(msg:String) = {
    println(getIdenty+"[INFO]: "+msg)
  }

  def warn(msg:String) = {
    println(getIdenty+"[WARN]: "+msg)
  }

}
