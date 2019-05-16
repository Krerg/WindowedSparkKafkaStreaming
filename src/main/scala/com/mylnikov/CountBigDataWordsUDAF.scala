package com.mylnikov

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

class CountBigDataWordsUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StringType(
    Array(StructField("value", StringType))
  )

  override def bufferSchema: StructType = StructType( Array(
    StructField("aggMap", MapType(StringType, IntegerType))
  )
  )

  override def dataType: DataType = StringType

  override def deterministic: Boolean = ???

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = mutable.Map[String, Integer]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getMap[String, Int](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}
