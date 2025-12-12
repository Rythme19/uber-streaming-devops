package com.example.producer

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaCSVProducerApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaBootstrap = sys.props.getOrElse("kafka.bootstrap", "localhost:9092")
    val topic          = sys.props.getOrElse("topic", "uber_topic")
    val csvPath        = sys.props.getOrElse("file", "ncr_ride_bookings.csv")
    val partitions     = sys.props.getOrElse("partitions", "8").toInt
    val sendDelayMs    = sys.props.getOrElse("delay.ms", "0").toInt

    // Read and repartition CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)
      .repartition(partitions)

    val rowCount = df.count()
    println(s" Loaded $rowCount rows from '$csvPath', repartitioned to $partitions partitions")

    // Collect to driver (safe for datasets < ~1M rows)
    val records = df.collect()
    val schema = df.schema

    // Kafka producer setup
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrap)
    props.put("acks", "1")
    props.put("linger.ms", "20")
    props.put("batch.size", (64 * 1024).toString)
    props.put("compression.type", "lz4")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val startTime = System.currentTimeMillis()

    try {
      records.zipWithIndex.foreach { case (row, idx) =>
        // Build JSON safely
        val fields = schema.fieldNames.map { field =>
          val value = Option(row.getAs[Any](field))
            .map(_.toString.replace("\"", "\\\"").replace("\n", " ").replace("\r", " "))
            .getOrElse("")
          s""""$field":"$value""""
        }
        val json = s"{${fields.mkString(",")}}"

        // Use vehicle_type as key (improves partition locality)
        val key = Option(row.getAs[String]("vehicle_type")).orNull

        producer.send(new ProducerRecord(topic, key, json))

        if (sendDelayMs > 0) Thread.sleep(sendDelayMs)

        if ((idx + 1) % 1000 == 0 || idx + 1 == records.length) {
          println(s" Sent ${idx + 1} / $rowCount records...")
        }
      }

      producer.flush()
      println(s" All records sent to Kafka topic: '$topic'")
    } catch {
      case e: Exception =>
        System.err.println(s" Error during Kafka send: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      producer.close()
      spark.stop()
    }

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"⏱️  Total time: ${"%.1f".format(duration)} seconds")
  }
}