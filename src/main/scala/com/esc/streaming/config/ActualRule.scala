package com.esc.streaming.config
import com.esc.streaming.message.{Attribute, ParseRegulation}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

/**
  * Created by zhangxing on 2017/6/22.
  */
class ActualRule(regulationJson:Map[String,List[(String,String)]]) extends RuleCenter{
  override def getAllRules = {
    implicit val formats = DefaultFormats
    regulationJson.flatMap{case (topicName,regulars) => {
      regulars.map{case (regularName,attributes) =>{
        ("topicName" -> topicName) ~ ("regularName" -> regularName) ~ ("attributes" -> getArributes(attributes).map{
            attr => (("name" -> attr.name) ~ ("ptype" -> attr.ptype) ~ ("index" -> attr.index) )
        })
      }}

    }}.map(s => s.extract[ParseRegulation])
//

  }

  def getArributes(attributes:String):List[Attribute] = {
     attributes.split(";").map(s => s.split(",") match {
       case Array(name,ptype,index) => Attribute(name,ptype,index.toInt)
     }).toList

  }
}

object ActualRule{



}
