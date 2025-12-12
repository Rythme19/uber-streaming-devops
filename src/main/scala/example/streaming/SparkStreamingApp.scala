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

    // Configuration from system properties (set via -D or -- in spark-submit)
    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val kafkaTopic     = sys.props.getOrElse("kafka.topic", "uber_topic")
    val parquetOut     = sys.props.getOrElse("parquet.out", "/tmp/uber_stream_output/parquet")

    // Define expected schema from Kafka JSON
    val schema = new StructType()
      .add("ride_id", StringType)
      .add("pickup_datetime", StringType)
      .add("dropoff_datetime", StringType)
      .add("vehicle_type", StringType)
      .add("passenger_count", IntegerType)
      .add("fare_amount", DoubleType)

    // Read from Kafka
    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Parse JSON
    val parsedDF = rawKafka
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    // Convert pickup time to timestamp (adjust format if needed)
    val dfWithTs = parsedDF
      .withColumn(
        "pickup_ts",
        to_timestamp($"pickup_datetime", "yyyy-MM-dd HH:mm:ss")
      )
      .filter($"pickup_ts".isNotNull)

    // Time-based aggregation with watermarking
    val aggDF = dfWithTs
      .withWatermark("pickup_ts", "2 minutes")
      .groupBy(
        window($"pickup_ts", "1 minute", "30 seconds"), // 1-min tumbling window, advanced every 30s
        $"vehicle_type"
      )
      .agg(
        count("*").alias("ride_count"),
        avg($"fare_amount").alias("avg_fare")
      )
      .select(
        $"window.start".alias("window_start"),
        $"window.end".alias("window_end"),
        $"vehicle_type",
        $"ride_count",
        $"avg_fare"
      )

    // Write aggregated stream to Parquet (append mode)
    val query = aggDF.writeStream
      .format("parquet")
      .option("path", parquetOut)
      .option("checkpointLocation", s"$parquetOut/../checkpoint")
      .outputMode("append")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()

    println(s" Spark streaming started!")
    println(s"   Kafka topic: $kafkaTopic")
    println(s"   Parquet output: $parquetOut")
    println(s"   Checkpoint: ${parquetOut}/../checkpoint")
    println("   Waiting for data... (Press Ctrl+C to stop)")

    spark.streams.awaitAnyTermination()
  }
}