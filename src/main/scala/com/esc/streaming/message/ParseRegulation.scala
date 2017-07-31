package com.esc.streaming.message

/**
  * Created by pc-admin on 2017/7/12.
  */
case class ParseRegulation(topicName:String, regularName:String,regx:String, window:Long,sql:String,attributes: List[Attribute])


