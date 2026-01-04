
---

## 2️⃣ Features

- Reads Uber ride bookings from a CSV file (`ncr_ride_bookings.csv`)
- Streams records into **Kafka** (`uber_topic`)
- Consumes and processes data with **Spark Structured Streaming**
- Aggregates rides by **vehicle type** and **sliding time window** (e.g., 5-minute windows)
- Writes aggregated results to **Parquet files** (columnar, compressed, query-friendly)
- Supports configurable output path via CLI argument (`--parquet.out`)

---

## 3️⃣ Prerequisites

- **Docker & Docker Compose** (for Kafka)
- **Java 11** (required by Spark 4.0.1; *not Java 17*)
- **sbt** (Scala Build Tool)
- CSV dataset: `ncr_ride_bookings.csv` (with columns like `timestamp`, `vehicle_type`, etc.)


## ▶️ How to Run

### 1. Start Kafka

```bash
docker-compose up -d

to create topics or reset them:

 run ./reset.sh 

sbt clean compile
sbt clean assembly

sbt "runMain com.example.producer.KafkaProducerApp"

or

spark-submit \
  --class com.example.producer.KafkaProducerApp \
  --master local[4] \
  target/scala-2.13/uber-streaming-devops-assembly-0.1.0-SNAPSHOT.jar \
  -Dfile=ncr_ride_bookings.csv \
  -Ddelay.ms=100


sbt "runMain com.example.streaming.SparkStreamingApp"

or

spark-submit \
  --class com.example.streaming.SparkStreamingApp \
  --master local[8] \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=8 \
  target/scala-2.13/uber-streaming-devops-assembly-0.1.0-SNAPSHOT.jar \
  -Dkafka.bootstrap=localhost:9092 \
  -Dkafka.topic=uber_topic \
  -Dparquet.out=/tmp/uber_stream_output/parquet