package com.mylnikov

import scala.collection.mutable

class MessageProcessor {

  val searchWords = Array("big data", "ai", "machine learning", "course")

  /**
    *
    * @param text
    * @return
    */
  def process(text: String): mutable.Map[String, Int] = {
    val aggregatorMap = mutable.Map[String, Int]()

    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy-MM-dd_HH-mm")

    TextAnalyzer.getWords(text, searchWords)
      .foreach(word => {
        aggregatorMap += (word -> (aggregatorMap.getOrElse(word, 0) + 1))
      })
    aggregatorMap
  }

}
