package com.adaltas.taxistreaming.processing

import java.sql.Timestamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLImplicits, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite}

trait SparkTestingSuite extends FlatSpec with BeforeAndAfterAll { self: Suite =>

  var sparkTest: SparkSession = _

  override def beforeAll() {
    super.beforeAll()
    sparkTest = SparkSession.builder().appName("Taxi processing testing")
    .master("local")
    .getOrCreate()
  }

  override def afterAll() {
    sparkTest.stop()
    super.afterAll()
  }

}

trait TaxiTestDataHelpers extends SparkTestingSuite {

  private val taxiRidesSchema = StructType(Array(
    StructField("rideId", LongType), StructField("isStart", StringType),
    StructField("endTime", TimestampType), StructField("startTime", TimestampType),
    StructField("startLon", FloatType), StructField("startLat", FloatType),
    StructField("endLon", FloatType), StructField("endLat", FloatType),
    StructField("passengerCnt", ShortType), StructField("taxiId", LongType),
    StructField("driverId", LongType))) // "yyyy-MM-dd hh:mm:ss" e.g. 2013-01-01 00:00:00

  private val taxiFaresSchema = StructType(Seq(
    StructField("rideId", LongType), StructField("taxiId", LongType),
    StructField("driverId", LongType), StructField("startTime", TimestampType),
    StructField("paymentType", StringType), StructField("tip", FloatType),
    StructField("tolls", FloatType), StructField("totalFare", FloatType)))

  def getDataRides(): List[String] = {
    List(
      "6,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.866135,40.771091,-73.961334,40.764912,6,2013000006,2013000006",
      "11,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.870834,40.773769,-73.792358,40.771759,1,2013000011,2013000011",
      "55,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.87117,40.773914,-73.805054,40.68121,1,2013000055,2013000055",
      "31,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.929344,40.807728,-73.979935,40.740757,2,2013000031,2013000031",
      "34,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.934555,40.750957,-73.916328,40.762241,5,2013000034,2013000034"
    )
  }

  def getDataFares(): List[String] = {
    List(
      "1,2013000001,2013000001,2013-01-01 00:00:00,CSH,0,0,21.5",
      "2,2013000002,2013000002,2013-01-01 00:00:00,CSH,0,0,7",
      "3,2013000003,2013000003,2013-01-01 00:00:00,CRD,2.2,0,13.7",
      "4,2013000004,2013000004,2013-01-01 00:00:00,CRD,1.7,0,10.7",
      "5,2013000005,2013000005,2013-01-01 00:00:00,CRD,4.65,0,20.15"
    )
  }

  def convTaxiRidesToDf(inputData: List[String]): DataFrame = {
    val rdd = sparkTest.sparkContext.parallelize(inputData) // RDD[String]
    val rddSplitted = rdd.map(_.split(",")) //RDD[Array[String]]
    val rddRows: RDD[Row] = rddSplitted.map(arr => Row(
      arr(0).toLong, arr(1), Timestamp.valueOf(arr(2)), Timestamp.valueOf(arr(3)),
      arr(4).toFloat, arr(5).toFloat, arr(6).toFloat, arr(7).toFloat,
      arr(8).toShort, arr(9).toLong, arr(10).trim.toLong)) //rowRideRDD

    sparkTest.createDataFrame(rddRows, taxiRidesSchema)
  }

}

class TaxiProcessingTest extends TaxiTestDataHelpers {

  "A ride starting OUTSIDE the east edge of NYC" should "be filtered out" in {
    val dfRides = convTaxiRidesToDf(getDataRides() :+
      "-1,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.6,40.771091,-73.961334,40.764912,6,2013000006,2013000006"
      // ride with -73.6 startLon appended is outside east edge of NYC and should be filtered out
    )
    assert(TaxiProcessing.cleanRidesOutsideNYC(dfRides).count() === 5)
  }

  "A ride starting ON the east edge of NYC" should "not be filtered out (edge case)" in {
    val dfRides = convTaxiRidesToDf(getDataRides() :+
      "-1,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.70001,40.771091,-73.961334,40.764912,6,2013000006,2013000006"
      // ride with -73.70001 startLon appended isn't outside NYC, it's an edge case that should work
    )
    assert(TaxiProcessing.cleanRidesOutsideNYC(dfRides).count() === 6)
  }

}
