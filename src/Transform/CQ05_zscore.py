from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
from pyspark.sql.functions import date_format, concat, lit
from pyspark.sql.functions import col, from_json, to_timestamp, struct, array, lit, to_json, expr, when, coalesce, explode,collect_list,to_utc_timestamp

CONFIG = {
    "KAFKA_TOPIC_PRICE": "btc-price",
    "KAFKA_TOPIC_MOVING": "btc-price-moving",
    "KAFKA_TOPIC_ZSCORE": "btc-price-zscore",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092" 
}

def init_spark():
    spark = SparkSession.builder \
        .appName("BTC Price Z-Score") \
        .config("spark.sql.codegen.methods.splitThreshold", "10000") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_kafka_topic(spark, topic):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df

def parse_price_stream(df):
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time", StringType(), True)
    ])
    df_parsed = (
        df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col("value"), schema).alias("data"))
          .select("data.*")
          .withColumn("timestamp", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
          .filter(col("price").isNotNull() & (col("price") > 0))
          .withWatermark("timestamp", "10 seconds")
    )
    return df_parsed

def parse_moving_stream(df):
    window_schema = StructType([
        StructField("window", StringType(), True),
        StructField("avg_price", DoubleType(), True),
        StructField("std_price", DoubleType(), True)
    ])
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("windows", ArrayType(window_schema), True)
    ])
    df_parsed = (
        df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col("value"), schema).alias("data"))
          .select("data.*")
          .withColumn("timestamp",to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
          .filter(col("windows").isNotNull())
          .withWatermark("timestamp", "10 seconds")
    )
    return df_parsed

def compute_zscores(price_df, moving_df):
    
    joined_df = price_df.join(
        moving_df,
        on=["timestamp", "symbol"],
        how="inner"
    )
    
    exploded_df = joined_df.select(
        col("timestamp"),
        col("symbol"),
        col("price"),
        explode(col("windows")).alias("window_stats")
    )
    
    zscore_df = exploded_df.select(
        col("timestamp"),
        col("symbol"),
        struct(
            col("window_stats.window").alias("window"),
            when(
                col("window_stats.std_price") == 0,
                lit(0.0)
            ).otherwise(
                (col("price") - col("window_stats.avg_price")) / col("window_stats.std_price")
            ).alias("zscore_price")
        ).alias("zscore")
    )
    # Group by timestamp and symbol to collect zscores into an array
    result_df = zscore_df.groupBy("timestamp", "symbol").agg(
        collect_list(col("zscore")).alias("zscores")
    )
    return result_df

def format_output(df):
    return df.select(
        col("symbol").cast("string").alias("key"),
        to_json(struct(
            to_utc_timestamp(to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), "UTC").alias("timestamp"),
            "symbol", 
            "zscores"
        )).alias("value")
    )

def write_to_kafka(df):
    query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("topic", CONFIG["KAFKA_TOPIC_ZSCORE"]) \
        .option("checkpointLocation", "tmp/checkpoint_price_zscore") \
        .outputMode("append") \
        .start()
    return query


if __name__ == "__main__":
    spark = init_spark()

    price_raw_df = read_kafka_topic(spark, CONFIG["KAFKA_TOPIC_PRICE"])
    moving_raw_df = read_kafka_topic(spark, CONFIG["KAFKA_TOPIC_MOVING"])

    price_df = parse_price_stream(price_raw_df)
    moving_df = parse_moving_stream(moving_raw_df)
    
    
    zscore_df = compute_zscores(price_df, moving_df)

    
    formatted_df = format_output(zscore_df)
    kafka_query = write_to_kafka(formatted_df)

    spark.streams.awaitAnyTermination()