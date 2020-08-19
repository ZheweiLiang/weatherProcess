package com.example.spark.configuration

case class SparkConfiguration(
  sparkMasterUrl: String,
  checkpointDirectory: String,
  timeoutInMinutes: Int,
  logLevel: String)
