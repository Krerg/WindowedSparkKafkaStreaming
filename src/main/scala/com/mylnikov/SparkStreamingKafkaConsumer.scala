package com.mylnikov

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object SparkStreamingKafkaConsumer {

  /**
    * For testing purposes.
    */
  var ssc: StreamingContext = _

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[MessageDeserializer],
      "group.id" -> "kafkaStreaming"
    )

    val sparkConf = new SparkConf()
      .setAppName("KafkaStreaming")
      //comment this in case deployment
      .setMaster("local[*]")
    ssc = new StreamingContext(sparkConf, Milliseconds(200))




  }
}
