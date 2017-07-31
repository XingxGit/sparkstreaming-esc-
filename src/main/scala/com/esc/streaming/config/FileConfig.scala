package com.esc.streaming.config

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by zhangxing on 2017/6/20.
  */
class FileConfig(filePath:String) extends ConfigCenter{
  var absolute = false
  private val properties = new Properties()
  override def getPropertie(key:String): String = {
    try {
      val path = getPath(filePath, absolute)
      properties.load(new FileInputStream(path))
      properties.getProperty(key, "")
    } catch {
      case ex:Exception => ""
    }
  }

  private def getPath(path:String,isAbsolute:Boolean):String = {
    if(isAbsolute)
      path
    else
      Thread.currentThread().getContextClassLoader.getResource(path).getPath
  }

}
