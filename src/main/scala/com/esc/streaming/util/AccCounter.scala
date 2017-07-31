package com.esc.streaming.util

import org.apache.spark.{Accumulator, SparkContext}

/**
  * Created by zhangxing on 2017/6/23.
  */
object AccCounter {

  @volatile private var instance: Accumulator[Int] = null

  def getInstance(sc: SparkContext): Accumulator[Int] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0)
        }
      }
    }
    instance
  }

}
