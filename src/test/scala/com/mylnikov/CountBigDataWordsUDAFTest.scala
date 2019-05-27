package com.mylnikov

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.scalatest.FunSuite

class CountBigDataWordsUDAFTest extends FunSuite {

  val countBigDataWordsUDAF = new CountBigDataWordsUDAF()

  test("Should aggregate messages") {
    val buffer = new DummyBuffer(new Array[Any](1))
    countBigDataWordsUDAF.initialize(buffer)

    var text: Array[Any] = Array("Some big data text with ai")
    countBigDataWordsUDAF.update(buffer, new DummyBuffer(text))
    text = Array("Another text with big data")
    countBigDataWordsUDAF.update(buffer, new DummyBuffer(text))
    assert("big data:2|ai:1" == countBigDataWordsUDAF.evaluate(buffer))
  }

  test("Should merge buffers") {
    val buffer = new DummyBuffer(new Array[Any](1))
    countBigDataWordsUDAF.initialize(buffer)

    var text: Array[Any] = Array("Some big data text with ai")
    countBigDataWordsUDAF.update(buffer, new DummyBuffer(text))
    text = Array("Another text with big data")
    countBigDataWordsUDAF.update(buffer, new DummyBuffer(text))
    assert("big data:2|ai:1" == countBigDataWordsUDAF.evaluate(buffer))
  }

  class DummyBuffer(init: Array[Any]) extends MutableAggregationBuffer {
    val values: Array[Any] = init
    def update(i: Int, value: Any): Unit = values(i) = value
    def get(i: Int): Any = values(i)
    def length: Int = init.length
    def copy() = new DummyBuffer(values)
  }


}

