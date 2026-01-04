package com.example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UberStreamingConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val kafkaTopic     = sys.props.getOrElse("kafka.topic", "uber_topic")
    val parquetOut     = sys.props.getOrElse("parquet.out", "/tmp/uber_stream_output/parquet")

    // ✅ Schéma exact basé sur les colonnes réelles du CSV
    val schema = new StructType()
      .add("Date", StringType)
      .add("Time", StringType)
      .add("Booking ID", StringType)
      .add("Booking Status", StringType)
      .add("Customer ID", StringType)
      .add("Vehicle Type", StringType)
      .add("Pickup Location", StringType)
      .add("Drop Location", StringType)
      .add("Avg VTAT", DoubleType)
      .add("Avg CTAT", DoubleType)
      .add("Cancelled Rides by Customer", IntegerType)
      .add("Reason for cancelling by Customer", StringType)
      .add("Cancelled Rides by Driver", IntegerType)
      .add("Driver Cancellation Reason", StringType)
      .add("Incomplete Rides", IntegerType)
      .add("Incomplete Rides Reason", StringType)
      .add("Booking Value", DoubleType)        // ← C'est le "fare"
      .add("Ride Distance", DoubleType)
      .add("Driver Ratings", DoubleType)
      .add("Customer Rating", DoubleType)
      .add("Payment Method", StringType)

    // Lecture Kafka
    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Parsing JSON
    val parsedDF = rawKafka
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    // ✅ Combiner Date + Time en un seul timestamp (format réel du CSV)
    val dfWithTs = parsedDF
      .withColumn("event_time_str", concat(col("Date"), lit(" "), col("Time")))
      .withColumn("event_ts", to_timestamp(col("event_time_str"), "yyyy-MM-dd HH:mm:ss"))
      .filter(col("event_ts").isNotNull)
      .drop("event_time_str")

    // ✅ Agrégation : compter les réservations valides par type de véhicule
    val aggDF = dfWithTs
      .filter(col("Booking Status") === "Completed") // Ignore cancelled/incomplete
      .withWatermark("event_ts", "2 minutes")
      .groupBy(
        window(col("event_ts"), "5 minutes"), // fenêtre de 5 min
        col("Vehicle Type")
      )
      .agg(
        count("*").alias("ride_count"),
        avg("Booking Value").alias("avg_fare"),
        avg("Ride Distance").alias("avg_distance")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("Vehicle Type").alias("vehicle_type"),
        col("ride_count"),
        col("avg_fare"),
        col("avg_distance")
      )

    // Écriture Parquet
    val query = aggDF.writeStream
      .format("parquet")
      .option("path", parquetOut)
      .option("checkpointLocation", s"$parquetOut/../checkpoint")
      .outputMode("append")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()

    println("✅ Spark streaming démarré !")
    println(s"   Topic Kafka : $kafkaTopic")
    println(s"   Sortie Parquet : $parquetOut")
    spark.streams.awaitAnyTermination()
  }
}