package com.mylnikov

import org.json4s.JObject
import org.json4s.jackson.JsonMethods

object SparkStreamingKafkaConsumer {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration(args)
    conf.verify()

    val spark = org.apache.spark.sql.SparkSession.builder
            .master("local[*]")
      .appName("SparkKafkaConsumer")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.bootstrapServer())
      .option("subscribe", conf.inputKafkaTopic())
      .option("startingOffsets", "earliest")
      .option("checkpointLocation", "/d/checkpoint")
      .load()

    import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._
    val getText: String => String = JsonMethods.parse(_).asInstanceOf[JObject].values.getOrElse("text", "").toString
    val getTextUdf = udf(getText)

    val messageProcessor = new MessageProcessor()

    val udaf = new CountBigDataWordsUDAF
    spark.udf.register("udaf", udaf)
    // Select key and extracted value
    df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .withWatermark("timestamp", "60 minutes")
      .withColumn("value", getTextUdf('value))
      //Group by window
      .groupBy(window('timestamp, "20 second"))
      // Count each word using UDAF
      .agg(udaf('value).as("WordsCount"))
      //Build the output
      .selectExpr("CAST(window as STRING)", "WordsCount")
      .withColumn("WordsCount", concat('WordsCount, lit(" Window:"), 'window))
      .selectExpr("CAST(window as String) as key","CAST(WordsCount AS STRING) as value")

      //Write the count to the kafka
      .writeStream.outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.bootstrapServer())
      .option("topic", conf.outputKafkaTopic())
      .option("checkpointLocation", conf.checkpointPath())
      .start().awaitTermination()
  }

}
