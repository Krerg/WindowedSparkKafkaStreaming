package com.mylnikov

import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s.JObject
import org.json4s.jackson.JsonMethods

object SparkStreamingKafkaConsumer {

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

    spark.udf.register("gm", new CountBigDataWordsUDAF)


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

    val udaf = new CountBigDataWordsUDAF

    df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS LONG)")
      .withWatermark("timestamp", "5 minutes")
      .withColumn("value", getTextUdf('value))
      .groupBy(window('timestamp, "60 minutes"))
      .agg(udaf('value))



  }
}
