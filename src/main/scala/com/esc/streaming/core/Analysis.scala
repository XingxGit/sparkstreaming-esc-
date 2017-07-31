package com.esc.streaming.core

import org.apache.spark.Accumulator


/**
  * Created by pc-admin on 2017/6/26.
  */
object Analysis {

  def countTotal(topic:String,CountName:String,num:Int,bg:Map[String,Map[String,Accumulator[Int]]]) = {
    val countMap = bg.get(topic).get
    if(countMap!=None&&countMap.isInstanceOf[Map[String,Accumulator[Int]]]){
      countMap.asInstanceOf[Map[String,Accumulator[Int]]].foreach{case (name,counter) => {
        if((CountName+"Count").equals(name)){
          counter.add(num)
        }
      }}
    }

  }

  def countWindowTotal(topic:String,CountName:String,num:Int,bg:Map[String,Map[String,Accumulator[Int]]]) = {
    val countMap = bg.get(topic).get
    if(countMap!=None&&countMap.isInstanceOf[Map[String,Accumulator[Int]]]) {
      countMap.asInstanceOf[Map[String, Accumulator[Int]]].foreach { case (name, counter) => {
        if ((CountName + "Count").equals(name)) {
          counter.add(num)
        }
      }
      }
    }
  }


}
