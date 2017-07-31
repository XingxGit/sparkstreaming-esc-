package com.esc.streaming.constant

/**
  * Created by zhangxing on 2017/6/26.
  */
object WarnLevel extends Enumeration{
      type WarnLevel = Value
      val DISMISS = Value("dismiss")
      val INFO = Value("info")
      val DEBUG = Value("debug")
      val WARN = Value("warn")
      val ERROR = Value("error")
      val NORMAL = Value("normal")
}
