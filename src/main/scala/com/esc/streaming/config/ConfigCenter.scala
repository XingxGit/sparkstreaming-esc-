package com.esc.streaming.config

/**
  * Created by zhangxing on 2017/6/20.
  */
trait ConfigCenter{
    def getPropertie(key:String):String
}
