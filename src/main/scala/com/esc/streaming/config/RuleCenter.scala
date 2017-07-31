package com.esc.streaming.config

import com.esc.streaming.regulation.{SingleWarnRule, CommonRule, TimeCountRule}

/**
  * Created by zhangxing on 2017/6/22.
  */
trait RuleCenter {

 def getAllRules:Any


}
