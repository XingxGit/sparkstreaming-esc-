package com.esc.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by pc-admin on 2017/7/13.
  */
class KafkaSource(ssc:StreamingContext,topics:Set[String]) {
//  val servers = "121.201.78.13:9092,121.201.78.37:9092,121.201.78.36:9092"
  val servers = "192.168.254.128:9092,192.168.254.129:9092,192.168.254.130:9092"
  val configMap = Map(("metadata.broker.list", servers),
//    ("zookeeper.session.timeout.ms","400"),
//    ("zookeeper.sync.time.ms","200"),
//    ("auto.commit.interval.ms","1000"),
//    ("auto.offset.reset" ,"smallest"),
    ("spark.streaming.kafka.maxRatePerPartition", "100"),
    ("group.id", "sparkstreaming"))
  def getDStream() = {
//    val zQ = "121.201.78.37:2181,121.201.78.36:2181,121.201.78.11:2181"
    val zQ = "192.168.254.128:2181,192.168.254.129:2181,192.168.254.130:2181"
    val groupid = "sparks"
    val km = new KafkaManager(configMap)
    val dStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,configMap,topics)

//    val dStream = KafkaUtils.createStream(ssc,zQ,groupid,topics.map((_,1)).toMap)
    (km,dStream)
  }

}
