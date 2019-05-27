package com.mylnikov

import org.rogach.scallop.ScallopConf

class Configuration(arguments: Seq[String]) extends ScallopConf(arguments) {

  val inputKafkaTopic = opt[String] (required = true)
  val bootstrapServer = opt[String] (required = true)
  val outputKafkaTopic = opt[String] (required = true)
  val checkpointPath = opt[String] (required = true)

}
