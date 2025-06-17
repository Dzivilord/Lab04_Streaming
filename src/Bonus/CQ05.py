from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, from_json, to_json, struct
import pandas as pd
import os
import logging


## Remove-Item -Path ".\src\Bonus\btc_state_buffer",".\src\Bonus\main_checkpoint" -Recurse -Force -ErrorAction SilentlyContinue: Làm sạch thư mục state buffer và checkpoint trước khi chạy lại


CONFIG = {
    "KAFKA_BOOTSTRAP": "localhost:9092",
    "KAFKA_TOPIC_SOURCE": "btc-price",
    "KAFKA_TOPIC_HIGHER": "btc-price-higher",
    "KAFKA_TOPIC_LOWER": "btc-price-lower",
    "STATE_PATH": "./src/Bonus/btc_state_buffer",
    "CHECKPOINT": "./src/Bonus/main_checkpoint"
}


def init_spark():
    os.environ["PYSPARK_PYTHON"] = "python"
    spark = SparkSession.builder \
        .appName("KafkaBTCConsumerSimple") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", FloatType()),
    StructField("timestamp", StringType())  # ISO 8601
])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_batch(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    if batch_df.isEmpty():
        logger.info("Empty batch. Skipping.")
        return

    try:
        current_batch = batch_df.toPandas()
        current_batch["event_time"] = pd.to_datetime(current_batch["event_time"])
        current_batch = current_batch.dropna(subset=["price", "event_time"])
        logger.info(f"Batch {batch_id} rows after cleaning: {len(current_batch)}")

        # Đọc state buffer cũ
        old_pandas = load_previous_state()

        # Gộp dữ liệu
        combined_df = pd.concat([old_pandas, current_batch], ignore_index=True) if not old_pandas.empty else current_batch
        combined_df = combined_df.sort_values(["symbol", "event_time"])

        # Tính toán khoảng thời gian higher/lower
        higher_records, lower_records = compute_windows(current_batch, combined_df)

        # Ghi Kafka
        write_to_kafka("higher", higher_records, CONFIG["KAFKA_TOPIC_HIGHER"])
        write_to_kafka("lower", lower_records, CONFIG["KAFKA_TOPIC_LOWER"])

        # Ghi state mới
        update_state_buffer(combined_df)

    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)

def compute_windows(current_batch, combined_df):
    higher_records = []
    lower_records = []

    for _, row in current_batch.iterrows():
        symbol = row['symbol']
        current_time = row['event_time']
        current_price = row['price']
        window_end = current_time + pd.Timedelta(seconds=20)

        window = combined_df[
            (combined_df["symbol"] == symbol) &
            (combined_df["event_time"] > current_time) &
            (combined_df["event_time"] <= window_end)
        ]

        # HIGHER
        higher_window = 20.0
        higher = window[window['price'] > current_price]
        if not higher.empty:
            first_higher = higher.sort_values('event_time').iloc[0]
            higher_window = (first_higher['event_time'] - current_time).total_seconds()

        # LOWER
        lower_window = 20.0
        lower = window[window['price'] < current_price]
        if not lower.empty:
            first_lower = lower.sort_values('event_time').iloc[0]
            lower_window = (first_lower['event_time'] - current_time).total_seconds()

        higher_records.append({
            "timestamp": current_time.isoformat(timespec='milliseconds'),
            "higher_window": round(higher_window, 3)
        })
        lower_records.append({
            "timestamp": current_time.isoformat(timespec='milliseconds'),
            "lower_window": round(lower_window, 3)
        })

    return higher_records, lower_records

def load_previous_state():
    try:
        if os.path.exists(CONFIG["STATE_PATH"]):
            df = spark.read.parquet(CONFIG["STATE_PATH"])
            pdf = df.toPandas()
            pdf["event_time"] = pd.to_datetime(pdf["event_time"])
            return pdf
    except Exception as e:
        logger.warning(f"State load failed: {e}")
    return pd.DataFrame(columns=["symbol", "event_time", "price"])

def update_state_buffer(df):
    cutoff_time = df["event_time"].max() - pd.Timedelta(seconds=20)
    latest = df[df["event_time"] >= cutoff_time]
    try:
        spark.createDataFrame(latest).repartition(1).write.mode("overwrite").parquet(CONFIG["STATE_PATH"])
        logger.info(f"Updated state buffer with {len(latest)} records")
    except Exception as e:
        logger.error(f"State write failed: {e}")

def write_to_kafka(name, records, topic):
    if not records:
        return
    try:
        if name == "higher":
            df = spark.createDataFrame(records).select(to_json(struct("timestamp", "higher_window")).alias("value"))
        elif name == "lower":
            df = spark.createDataFrame(records).select(to_json(struct("timestamp", "lower_window")).alias("value"))
        else:
            logger.warning(f"Unknown record type: {name}")
            return

        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", CONFIG["KAFKA_BOOTSTRAP"]) \
            .option("topic", topic) \
            .mode("append") \
            .save()

        logger.info(f"Wrote {len(records)} {name} records to Kafka")
    except Exception as e:
        logger.error(f"Kafka write failed ({name}): {e}")


if __name__ == "__main__":
    spark = init_spark()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG["KAFKA_BOOTSTRAP"]) \
        .option("subscribe", CONFIG["KAFKA_TOPIC_SOURCE"]) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select(
        col("data.symbol"),
        col("data.price"),
        col("data.timestamp").cast("timestamp").alias("event_time")
    ).filter(col("price").isNotNull())

    watermarked_df = parsed_df.withWatermark("event_time", "10 seconds")

    query = watermarked_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", CONFIG["CHECKPOINT"]) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()
