name := "Process"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"% "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "provided"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.41"
libraryDependencies += "redis.clients" % "jedis" % "2.9.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0"
libraryDependencies += "org.json4s" % "json4s-jackson_2.10" % "3.5.2"

//libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.5"

