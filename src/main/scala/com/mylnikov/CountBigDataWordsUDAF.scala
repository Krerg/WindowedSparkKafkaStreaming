package com.mylnikov

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType, IntegerType, DataType}

import scala.collection.mutable

class CountBigDataWordsUDAF extends UserDefinedAggregateFunction {

  val searchWords = Array("big data", "ai", "machine learning", "course")

  override def inputSchema: StructType = StructType(
    Array(StructField("value", StringType))
  )

  override def bufferSchema: StructType = StructType( Array(
    StructField("aggMap", MapType(StringType, IntegerType))
  )
  )

  override def dataType: DataType = StringType

  override def deterministic: Boolean = {
    true
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = mutable.Map[String, Integer]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    TextAnalyzer.getWords(input.getAs[String]("value"), searchWords)
      .foreach(word => {
        buffer.getMap[String, Int](0) += (word -> (buffer.getMap[String, Int](0).getOrElse(word, 0) + 1))
      })
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getMap[String, Int](0) ++ buffer2.getMap[String, Int](0).map{ case (k, v) => k -> (v + buffer1.getMap[String, Int](0).getOrElse(k, 0)) }
  }

  override def evaluate(buffer: Row): Any = {

  }
}
