package com.mylnikov

import scala.collection.mutable

object TextAnalyzer {

  /**
    * Return  words that matches input list
    *
    * @param text text to search
    * @param wordsToSearch matched words
    * @return
    */
  def getWords(text: String, wordsToSearch: Array[String]): mutable.MutableList[String] = {
    var findWords: mutable.MutableList[String] = mutable.MutableList[String]()
    for (words <- wordsToSearch) {
      val reg = words.r
      findWords = findWords ++ reg.findAllIn(text).toList
    }
    findWords
  }

}