rm -rf checkpoint && sbt clean && sbt compile && sbt "runMain com.example.spark.SparkExample"