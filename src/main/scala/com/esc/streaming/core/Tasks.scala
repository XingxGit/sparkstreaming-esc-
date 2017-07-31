package com.esc.streaming.core

import com.esc.streaming.config.ActualRule
import com.esc.streaming.kafka.KafkaManager
import com.esc.streaming.message.ParseRegulation
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.JavaConverters._


/**
  * Created by pc-admin on 2017/6/28.
  */
object Tasks {
  var count = 0

  def getSTType(ty:String) = {
    ty match {
      case "int"  => IntegerType
      case "string" => StringType
    }
  }

  def changeType(tp:String,data:String) = {
    tp match {
      case "int"  => data.toInt
      case "string" => data
    }
  }

  def run(km:KafkaManager,lines:InputDStream[(String,String)],topic:String,sqlSC:SQLContext) = {
//    lines.persist(StorageLevel.MEMORY_AND_DISK_SER)
    lines.cache()
//    lines.print()
//    val sc = lines.context.sparkContext
//    val sqlSC = new SQLContext(sc)
    //
    lines.foreachRDD(rdd =>{
      if (!rdd.isEmpty){
        km.updateZKOffsets(rdd)
      }
    })
      lines.window(Seconds(10)).foreachRDD(rdd => {

        val ut = GlobalValue.getBC.value.filter(p => p.topicName.equals(topic))
        ut.foreach(pr => {
          val regx = pr.regx
          val attrs = pr.attributes
          val schema = StructType(attrs.map(attr => {
            StructField(attr.name, getSTType(attr.ptype), true)
          }))

        val rowRDD = rdd.map(record => {
          val find = regx.r.findAllIn(record._2).toList
          Row(attrs.map(attr => {
            try{
              changeType(attr.ptype,find(attr.index))
            }catch {
              case ex:IndexOutOfBoundsException => null
            }
          }):_*)
        })
        val regularDF = sqlSC.createDataFrame(rowRDD, schema)
        regularDF.registerTempTable(pr.regularName)
        sqlSC.sql(pr.sql).show()
      })
    })


    //    var uts:Broadcast[String]  = ut
    //commonRule 主要使用正则表达式来解析字符串
    //    ActualRule(topicsSet).getCommonRule match {
    //      case Some(commonRule) => {
    //        lines.map(record => {
    //          commonRule.regexs.map{case (name,regx) =>{
    //            regx.r.findFirstIn(record).map((name,_))
    //          }}
    //        }).foreachRDD(rdd => {
    //          val producer = KafkaSend.getInstance()
    //          rdd.foreach(results =>{
    //            val lb = new ListBuffer[String]
    //            results.foreach(x => x match {
    //              case Some(info) => lb.append(info._1+":"+info._2)
    //              case _ =>
    //            }
    //            )
    //            val message = new ProducerRecord[String, String]("receive", lb.mkString(","))
    //            producer.send(message)
    //          })
    //            producer.close()
    //        })
    //
    //      }
    //      case None =>
    //    }


    //    val updateValue = (now: Seq[Event],old:Option[Event]) => {
    //      var stat = "NORMAL"
    //      for(event <- now if event.level.equals("warn")) stat = "WARN"
    //      Some(Event("","",""))
    //    }
    //
    //    lines.map(x => (x,Event("warn","fred",TimeUtil.getNow))).updateStateByKey[Event](updateValue)


    //    val ss = updateValue(Seq(),"fred")


    //CountRule 用来不限时的全局变量计数
    //    ActualRule(topicsSet).getCountRule match {
    //      case Some(countRule) => {
    //        val countResult = lines.foreachRDD(rdd => {
    //         val result = rdd.map(record => {
    //            countRule.countVariable.map(countName => {
    //              val countList = uts.value.r.findAllIn(record).toList
    //              (uts.value, countList.size)
    //            })
    //          }).collect()
    //          println(result.mkString(","))
    //        })
    //
    //
    //
    //      }
    //      case None =>
    //    }

    //定义可查询数据结构
    //    val schema = StructType(Array(StructField("name",StringType,true),StructField("age",IntegerType,true)))
    //
    //    //TimeCountRule 用来做一段时间之内的全局变量计数
    //    ActualRule(topicsSet).getTimeCountRule match {
    //      case Some(timeCountRule) => {
    //        val countNames = timeCountRule.getTimeCountVariable
    //        val countTime = timeCountRule.getTime
    //        val picks = lines.map(record => {
    //          countNames.map(s => (s,s.r.findAllIn(record).toList.size) )
    //        })
    //        val windowCount = picks.flatMap(x => x).reduceByKeyAndWindow(_+_,_-_,Seconds(countTime),Seconds(2))
    //
    //        val sc = windowCount.context.sparkContext
    //        val sqlSC = new SQLContext(sc)
    //
    //
    //        windowCount.foreachRDD(rdd =>{
    ////          rdd.foreach(x => Analysis.countWindowTotal(topicsSet,x._1,x._2,bg))
    //          val rowRDD = rdd.map(p => Row(p._1,p._2))
    //          val peopleDF = sqlSC.createDataFrame(rowRDD,schema)
    //          peopleDF.registerTempTable("people")
    //          sqlSC.sql("select name,age from people").show()
    //        })
    //
    //        windowCount.print()
    //
    //      }
    //      case None =>
    //    }
    //
    //
  }

  def stop(topicsSet:String)  = {

  }
}
