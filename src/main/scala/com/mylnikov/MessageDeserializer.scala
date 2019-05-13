package com.mylnikov

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.mylnikov.model.Message
import org.apache.kafka.common.serialization.Deserializer

class MessageDeserializer extends Deserializer[Message] {

  val objectMapper = new ObjectMapper()

  val EMPTY_MESSAGE = new Message()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  override def deserialize(topic: String, data: Array[Byte]): Message = {
    try {objectMapper.readValue(data, classOf[Message])}
    catch {
      case ex: Exception => EMPTY_MESSAGE
    }
  }

  override def close(): Unit = {

  }
}

