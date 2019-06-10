package com.mylnikov.perfomance

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.fasterxml.jackson.databind.ObjectMapper
import com.mylnikov.model.Message
import org.scalatest.{BeforeAndAfter, FunSuite}

class JSONAndKryoPerfTest extends FunSuite with BeforeAndAfter {

  val objectMapper = new ObjectMapper()

  var kryo: Kryo = null

  val testObject = Message(text = "The world's longest place name belongs to a hill near Porangahau in the southern Hawke’s Bay in New Zealand. " +
    "It is a name given in the Maori language. This hill is 305 meters tall and is famously known for its long name. " +
    "This name has since been shortened to Taumata for the ease of pronunciation. " +
    "The meaning of the name has been translated to mean “the place where Tamatea, the man who had big knees, the climber of mountains, the slider, " +
    "the land-swallower that traveled about, played the nose flute that he had to the loved ones.” " +
    "With 85 characters, it is the longest place name in the world according to the Guinness World Records.",
    userName = "John1234u98@@@@@@@kkkkkkkkk",
    location = "Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu, New Zealand.")

  before {
    kryo = new Kryo()
  }

  test("Inmemory serialization performance test") {
    kryo.setRegistrationRequired(true)
    kryo.register(classOf[Message])
    val baos  = new ByteArrayOutputStream(777)
    val output = new Output(baos)
    val input = new Input(777)
    val kryoStartTime = System.currentTimeMillis()
    for(i <- 1 to 3000000) {
      kryo.writeObject(output, testObject)
      output.flush()
      input.setInputStream(new ByteArrayInputStream(output.getBuffer))
      val deserializedObject = kryo.readObject(input, classOf[Message])
      assert(deserializedObject != null)
      baos.reset()
    }
    val kryoEndTime = System.currentTimeMillis()

    val jsonStartTime = System.currentTimeMillis()
    for(i <- 1 to 3000000) {
      val serializedObject = objectMapper.writeValueAsBytes(testObject)
      val deserializedObject = objectMapper.readValue(serializedObject,classOf[Message])
      assert(deserializedObject != null)
    }
    val jsonEndTime = System.currentTimeMillis()

    val kryoTime = kryoEndTime - kryoStartTime
    val jsonTime = jsonEndTime - jsonStartTime

    println(s"Kryo time:$kryoTime | Json time:$jsonTime")
  }

  test("File serialization performance test") {

  }

}
