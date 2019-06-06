package com.adaltas.taxistreaming

import com.adaltas.taxistreaming.processing.TaxiProcessing
import com.adaltas.taxistreaming.utils.ParseKafkaMessage
import com.adaltas.taxistreaming.utils.StreamingDataFrameWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/*
spark-submit \
  --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 1 \
  --executor-memory 5g --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
  --conf spark.sql.hive.thriftServer.singleSession=true \
  target/scala-2.11/taxi-streaming-scala_2.11-0.1.0-SNAPSHOT.jar

* The application reads data from Kafka topic, parses Kafka messages, processes it, and prints the results in console
* `TipsInConsole` query writes the streaming results to stdout of the Spark Driver
* It takes a while for the batches to be processed and printed. To speed it up, the application would need more resources
* If you are interested only in development, you could submit the application without Hadoop, on Spark local (refer to part 1 of the series)
* Or submit the application on a real Hadoop cluster with more resources

*/

object MainConsole {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark Streaming part 3: testing")
      .getOrCreate()

    val taxiRidesSchema = StructType(Array(
      StructField("rideId", LongType), StructField("isStart", StringType),
      StructField("endTime", TimestampType), StructField("startTime", TimestampType),
      StructField("startLon", FloatType), StructField("startLat", FloatType),
      StructField("endLon", FloatType), StructField("endLat", FloatType),
      StructField("passengerCnt", ShortType), StructField("taxiId", LongType),
      StructField("driverId", LongType)))

    val taxiFaresSchema = StructType(Seq(
      StructField("rideId", LongType), StructField("taxiId", LongType),
      StructField("driverId", LongType), StructField("startTime", TimestampType),
      StructField("paymentType", StringType), StructField("tip", FloatType),
      StructField("tolls", FloatType), StructField("totalFare", FloatType)))

    //master02.cluster:6667 <-> localhost:9092
    var sdfRides = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "taxirides").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    var sdfFares= spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "taxifares").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    sdfRides = ParseKafkaMessage.parseDataFromKafkaMessage(sdfRides, taxiRidesSchema)
    sdfFares= ParseKafkaMessage.parseDataFromKafkaMessage(sdfFares, taxiFaresSchema)

    sdfRides = TaxiProcessing.cleanRidesOutsideNYC(sdfRides)
    sdfRides = TaxiProcessing.removeUnfinishedRides(sdfRides)
    var sdf = TaxiProcessing.joinRidesWithFares(sdfRides, sdfFares)
    sdf = TaxiProcessing.appendStartEndNeighbourhoods(sdf, spark)

    // Write streaming results in console
    StreamingDataFrameWriter.StreamingDataFrameConsoleWriter(sdf, "TipsInConsole").awaitTermination()

    spark.stop()
  }

}
