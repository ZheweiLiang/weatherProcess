package codeChallenge

import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object weatherProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("codeChallenge.weatherProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val weatherName = "data/2019/"
    val stationListPath = "stationlist.csv"
    val countryListPath = "countrylist.csv"

    // load files into memory
    val weatherRDD = sc.textFile(weatherName)
    val headerWeather = weatherRDD.first()
    val weatherDataRDD = weatherRDD.filter(_ != headerWeather).map(_.split(","))

    val stationRDD = sc.textFile(stationListPath)
    val headerStation = stationRDD.first()
    val stationDataRDD = stationRDD.filter(_ != headerStation).map(_.split(","))

    val countryRDD = sc.textFile(countryListPath)
    val headerCountry = countryRDD.first()
    val countryDataRDD = countryRDD.filter(_ != headerCountry).map(_.split(","))


    //    weatherDataRDD.foreach(println)
    //    stationDataRDD.foreach(println)
    //    countryDataRDD.foreach(println)

    val weathers = weatherDataRDD.map(x => (x(0), (x(0), x(1), x(2), x(3), x(11))))
    val stations = stationDataRDD.map(y => (y(1), y(0)))
    val countries = countryDataRDD.map(z => (z(0), z(1)))

    val stationsCountriesRDD = stations.join(countries)
    stationsCountriesRDD.foreach(println)

    val stationsCountriesNewKeyRDD = stationsCountriesRDD.map(x => (x._2._1, (x._1, x._2._2)))
    stationsCountriesNewKeyRDD.foreach(println)

    val weathersWithCountriesRDD = weathers.join(stationsCountriesNewKeyRDD)

    weathersWithCountriesRDD.cache()

    val temperatureAvg = weathersWithCountriesRDD.filter(_._2._1._5 != "9999.9").map(x => (x._1, (x._2._1._5, x._2._2._2))).
      mapValues(x => (x._1.toDouble, x._2, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2, x._3 + y._3)).
      mapValues(x => (x._1 / x._3, x._2)).sortBy(x => x._2._1, false, 1).take(2).tail(0)

    println(temperatureAvg)

    val dayRDD = weathersWithCountriesRDD.map(x => (x._1, (x._2._1._3, x._2._2._2)))
    var calendar = Calendar.getInstance()
    val format = new SimpleDateFormat("yyyyMMdd")

    val consectiveDayRDD = dayRDD.groupByKey().flatMapValues(
      it => {
        val dateSorted = it.iterator.toSet.toList.sorted
        var index: Int = 0
        dateSorted.map(x => {
          calendar.setTime(format.parse(x._1))
          calendar.add(Calendar.DAY_OF_YEAR, -index)
          index += 1
          (x, format.format(calendar.getTime))
        })
      }
    )

    val resultRDD = consectiveDayRDD.map(x => {
      ((x._1, x._2._2), x._2._1) //((userId,连续起始登陆时间)，登陆时间)
    }).groupByKey().mapValues(it => {
      val sorted = it.toList.sorted
      (sorted.size, sorted.head, sorted.last)
    }).filter(x => x._2._1 >= 1)
      .map(t => (t._1._1, t._2._1, t._2._2._2, t._2._3))

    println("The result is:")
    resultRDD.foreach(println)

    sc.stop()

  }

}
