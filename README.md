# Spark Project With Scala

You need to have Spark set up locally or remotely.

1. Edit the config:

  ```
  spark {
    spark-master-url = "local[*]"
    checkpoint-directory = "" 
    timeout-in-minutes = 5
  }
  ```



2. Start up [netcat] on the same port you pass the program.

```shell script
nc -l  8888
```

3. Take the data from resources/sample-data.txt and send them via netcat. 


```shell script
sbt "runMain com.yuq.spark.structuredstateful.StatefulStructuredSessionization"
```
