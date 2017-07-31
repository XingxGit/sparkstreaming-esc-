package com.esc.streaming.connect

/**
  * Created by zhangxing on 2017/6/20.
  */
trait Connector {

  def init:Connector

  def connect:Any

  def close

}
