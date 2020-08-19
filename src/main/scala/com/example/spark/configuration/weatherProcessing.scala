package com.example.spark.configuration

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

object weatherProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("codeChallenge.weatherProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val weatherPath = "./data/2019/"
    val stationListPath = "./stationlist.csv"
    val countryListPath = "./countrylist.csv"

    // load files into memory,
    // weather data, split by ","
    val weatherRDD = sc.textFile(weatherPath)
    val headerWeather = weatherRDD.first()
    val weatherDataRDD = weatherRDD.filter(_ != headerWeather).map(_.split(","))

    // stations data, split by ",", omit header
    val stationRDD = sc.textFile(stationListPath)
    val headerStation = stationRDD.first()
    val stationDataRDD = stationRDD.filter(_ != headerStation).map(_.split(","))

    // countries data, split by ",", omit header
    val countryRDD = sc.textFile(countryListPath)
    val headerCountry = countryRDD.first()
    val countryDataRDD = countryRDD.filter(_ != headerCountry).map(_.split(","))

    // get the weather data by with index STN, and values(STN_NO, WBAN, YEARMODA, TEMP, WDSP, FRSHTT)
    val weathers = weatherDataRDD.map(x => (x(0), (x(0), x(1), x(2), x(3), x(8), x(15))))
    // transfer station to (COUNTRY_ABBR, STN_NO)
    val stations = stationDataRDD.map(y => (y(1), y(0)))
    // transfer country to (COUNTRY_ABBR, COUNTRY_FULL)
    val countries = countryDataRDD.map(z => (z(0), z(1)))

    // join station and country with COUNTRY_ABBR
    val stationsCountriesRDD = stations.join(countries)
//    stationsCountriesRDD.foreach(println)

    // transfer station land country to a new key (STN_NO, (COUNTRY_ABBR, COUNTRY_FULL))
    val stationsCountriesNewKeyRDD = stationsCountriesRDD.map(x => (x._2._1, (x._1, x._2._2)))
//    stationsCountriesNewKeyRDD.foreach(println)

    // join weather with station and country with STN_NO
    val weathersWithCountriesRDD = weathers.join(stationsCountriesNewKeyRDD)

    weathersWithCountriesRDD.cache()

    // filter temperature by the missing ("9999.9"), transfer the RDD to (STN, (TEMP,COUNTRY_FULL)),
    // get mapValues (TEMP, COUNTRY, 1), then reduceByKey(TEMP1+TEMP2, COUNTRY, Number1 + Number2),
    // get mapValue(SUM TEMP/ SUM Number) to average TEMP,
    val temperatureAvg = weathersWithCountriesRDD.filter(_._2._1._4 != "9999.9").map(x => (x._1, (x._2._1._4, x._2._2._2))).
      mapValues(x => (x._1.toDouble, x._2, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2, x._3 + y._3)).
      mapValues(x => (x._1 / x._3, x._2))

    // sort by ascending with numPartitions =1.
    // get the first one for the hottest average mean temperature country
    val temperatureFirst = temperatureAvg.sortBy(x => x._2._1, false, 1).first()
    println("The hottest average mean temperature country: " + temperatureFirst)
//    The hottest average mean temperature country: (410300,(103.0,SAUDI ARABIA))

    // sort by descending with numPartitions = 1
    // get the first one for the coldest average mean temperature country
    val temperatureLast = temperatureAvg.sortBy(x => x._2._1, true, 1).first()
    println("The coldest average mean temperature country: " + temperatureLast)
//    The coldest average mean temperature country: (895770,(-72.3063186813187,ANTARCTICA))

    // similar to former ones, only consider WDSP, after sorting, take first two and get the second one
    val windSpeedAvg = weathersWithCountriesRDD.filter(_._2._1._5 != "999.9").map(x => (x._1, (x._2._1._5, x._2._2._2))).
      mapValues(x => (x._1.toDouble, x._2, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2, x._3 + y._3)).
      mapValues(x => (x._1 / x._3, x._2)).sortBy(x => x._2._1, false, 1).take(2).tail(0)
    println("The second highest average mean wind speed country: " + windSpeedAvg)
//    The second highest average mean wind speed country: (898640,(31.794163424124502,ANTARCTICA))

    // get dayRDD by (STN, (YEARMODA, COUNTRY))
    val dayRDD = weathersWithCountriesRDD.map(x => (x._1, (x._2._1._3, x._2._2._2)))
    var calendar = Calendar.getInstance()
    val format = new SimpleDateFormat("yyyyMMdd")

    // get consecutiveRDD by (STN, START DAY, End day )
    val consecutiveDayRDD = dayRDD.groupByKey().flatMapValues(
      iterator => {
        val daySorted = iterator.iterator.toSet.toList.sorted
        var index: Int = 0
        // in each group, minus index for them, to get the start day
        daySorted.map(start => {
          calendar.setTime(format.parse(start._1))
          calendar.add(Calendar.DAY_OF_YEAR, -index)
          index += 1
          (start, format.format(calendar.getTime))
        })
      }
    )

    // final result for counting the consecutive days.
   val resultRDD = consecutiveDayRDD.map(x => {
      ((x._1, x._2._2), x._2._1) //((STN, START DAY)，Consecutive Days)
    }).groupByKey().mapValues(iterator => {
      val sortedDay = iterator.toList.sorted
      (sortedDay.size, sortedDay.head, sortedDay.last)
    }).map(t => (t._1._1, t._2._1, t._2._2._2, t._2._3))

    println("The result is:")
    resultRDD.foreach(println)

//    (804720,3,VENEZUELA,(20190716,VENEZUELA))
//    (021490,182,SWEDEN,(20191231,SWEDEN))
//    (869190,45,BRAZIL,(20191224,BRAZIL))
//    (416780,1,PAKISTAN,(20190709,PAKISTAN))
//    (965050,2,INDONESIA,(20190102,INDONESIA))
//    (101470,365,GERMANY,(20191231,GERMANY))

    sc.stop()

  }

}
