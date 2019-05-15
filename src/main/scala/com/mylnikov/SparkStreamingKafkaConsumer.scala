package com.mylnikov


import com.mylnikov.model.Message
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Minutes, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.json4s.JObject
import org.json4s.jackson.JsonMethods

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

    val spark = org.apache.spark.sql.SparkSession.builder
            .master("local[2]")
      .appName("SparkKafkaConsumer")
      .getOrCreate

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()

    import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._
    val getText: String => String = JsonMethods.parse(_).asInstanceOf[JObject].values.getOrElse("text", "").toString
    val getTextUdf = udf(getText)

    val messageProcessor = new MessageProcessor()

    val window = Window.partitionBy("ProductType").orderBy("Quantity")

    df.selectExpr("CAST(key AS STRING)", "CAST(timestamp AS LONG)")
      .withWatermark("timestamp", "5 minutes")
      .withColumn("value", getTextUdf('value))
      .map(row => {messageProcessor.process(row.getAs[String]("value")) })
      .
      .groupBy(
      window('timestamp, "60 minutes"),
    ).
    val topics = Array(conf.inputKafkaTopic())

    val stream = KafkaUtils.createDirectStream[String, Message](
      ssc,
      PreferConsistent,
      Subscribe[String, Message](topics, kafkaParams)
    )


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
