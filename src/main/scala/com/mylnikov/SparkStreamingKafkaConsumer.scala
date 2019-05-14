package com.mylnikov


import com.mylnikov.model.Message
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Minutes, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamingKafkaConsumer {

  /**
    * For testing purposes.
    */
  var ssc: StreamingContext = _

  def main(args: Array[String]): Unit = {

    val conf = new Configuration(args)
    conf.verify()

    conf.inputKafkaTopic()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.bootstrapServer(),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[MessageDeserializer],
      "group.id" -> "kafkaStreaming"
    )

    val sparkConf = new SparkConf()
      .setAppName("KafkaStreaming")
      //comment this in case deployment
      .setMaster("local[*]")
    ssc = StreamingContext.getOrCreate("dir", () => {
      new StreamingContext(sparkConf, Milliseconds(20))
    })

    val topics = Array(conf.inputKafkaTopic())

    val stream = KafkaUtils.createDirectStream[String, Message](
      ssc,
      PreferConsistent,
      Subscribe[String, Message](topics, kafkaParams)
    )
    val messageProcessor = new MessageProcessor()

    stream.window(Minutes(60)).map(record => {
      messageProcessor.process(record.value().text)
    }).reduceByWindow((map1, map2) => {
      map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) }
    },
      Minutes(60),
      Minutes(60)).foreachRDD(rdd => {
      rdd.foreach(map => {
        //send to kafka thats all))
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
