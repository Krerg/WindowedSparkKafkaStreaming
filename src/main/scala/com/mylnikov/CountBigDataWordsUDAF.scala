package com.mylnikov

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType, IntegerType, DataType}

import scala.collection.mutable

/**
  * Custom UDAF to aggregate words in the masseges and count big data words.
  */
class CountBigDataWordsUDAF extends UserDefinedAggregateFunction {

  val searchWords = Array("big data", "ai", "machine learning", "course")

  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", StringType) :: Nil)

  /**
    * Initialize buffer schema to store aggregated words.
    */
  override def bufferSchema: StructType = StructType(
    StructField("aggMap", MapType(StringType, IntegerType)) :: Nil
  )

  override def dataType: DataType = StringType

  override def deterministic: Boolean = {
    true
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = mutable.Map[String, Integer]()
  }

  /**
    * Updates the buffer with given message
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    TextAnalyzer.getWords(input.getString(0), searchWords)
      .foreach(word => {
        buffer(0) = buffer.getMap[String, Int](0) + (word -> (buffer.getMap[String, Int](0).getOrElse(word, 0) + 1))
      })
  }

  /**
    * Merges 2 buffer by sum the words count
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.getMap[String, Int](0).isEmpty) {
      buffer1(0) = buffer2.getMap[String, Int](0)
      return
    }
      buffer1(0) = buffer1.getMap[String, Int](0) ++
        buffer2.getMap[String, Int](0)
          .map{ case (k, v) => k -> (v + buffer1.getMap[String, Int](0).getOrElse(k, 0)) }
  }

  /**
    * Returns the string representation of buffer (eg. 'bigdata:5|ai:7')
    */
  override def evaluate(buffer: Row): String = {
    buffer.getMap[String, Int](0).map { case (k, v) => k + ":" + v }.mkString("|")
  }
}
