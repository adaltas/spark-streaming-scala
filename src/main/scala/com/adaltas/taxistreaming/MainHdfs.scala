package com.adaltas.taxistreaming

import com.adaltas.taxistreaming.processing.TaxiProcessing
import com.adaltas.taxistreaming.utils.ParseKafkaMessage
import com.adaltas.taxistreaming.utils.StreamingDataFrameWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/*
spark-submit \
  --master yarn --deploy-mode client \
  --class com.adaltas.taxistreaming.MainHdfs \
  --num-executors 2 --executor-cores 1 \
  --executor-memory 5g --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
  --conf spark.sql.hive.thriftServer.singleSession=true \
  /vagrant/taxi-streaming-scala_2.11-0.1.0-SNAPSHOT.jar

* The application reads data from Kafka topic, parses Kafka messages, and dumps unaltered raw data to HDFS
* Two streaming queries
    * `PersistRawTaxiRides` query persists raw taxi rides data on hdfs path /user/spark/datalake/RidesRaw
    * `PersistRawTaxiFares` query persists raw taxi fares data on hdfs path /user/spark/datalake/FaresRaw

*/

object MainHdfs {

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

    var sdfRides = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "master02.cluster:6667").
      option("subscribe", "taxirides").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    var sdfFares= spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "master02.cluster:6667").
      option("subscribe", "taxifares").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    sdfRides = ParseKafkaMessage.parseDataFromKafkaMessage(sdfRides, taxiRidesSchema)
    sdfFares= ParseKafkaMessage.parseDataFromKafkaMessage(sdfFares, taxiFaresSchema)

    // Write raw data in HDFS
    StreamingDataFrameWriter.StreamingDataFrameHdfsWriter(sdfRides, "PersistRawTaxiRides")
    StreamingDataFrameWriter.StreamingDataFrameHdfsWriter(sdfFares, "PersistRawTaxiFares").awaitTermination()

    spark.stop()
  }

}
