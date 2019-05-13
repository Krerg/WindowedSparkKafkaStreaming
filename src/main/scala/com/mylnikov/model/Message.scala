package com.mylnikov.model

import scala.beans.BeanProperty

case class Message(@BeanProperty userName: String = null,
                   @BeanProperty location: String = null,
                   @BeanProperty text: String = null) {

  def this() {
    this("","","")
  }

}