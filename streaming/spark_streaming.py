"""
Spark Structured Streaming: Kafka(news.raw) → Iceberg (bronze / silver)
-----------------------------------------------------------------------
- bronze: 원천 이벤트를 거의 그대로 적재 (감사/재처리용)
- silver: 정제 이벤트 — 본문 HTML 태그 제거, 발행시각 파싱, 결측 처리,
          기사 id 기준 중복 제거(dropDuplicates + watermark)

카탈로그는 Iceberg REST Catalog(demo)를 사용하며, 스토리지는 MinIO(S3).
스트리밍은 foreachBatch로 마이크로배치마다 MERGE(멱등 upsert)를 수행한다.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "demo"
DB = "news"
BRONZE = f"{CATALOG}.{DB}.bronze_news"
SILVER = f"{CATALOG}.{DB}.silver_news"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news.raw")

# Producer가 발행하는 이벤트 스키마
EVENT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("source", StringType()),
    StructField("title", StringType()),
    StructField("summary", StringType()),
    StructField("link", StringType()),
    StructField("published_raw", StringType()),
    StructField("ingested_at", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("news-streaming")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", os.getenv("WAREHOUSE", "s3://warehouse/"))
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", CATALOG)
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            id STRING, source STRING, title STRING, summary STRING,
            link STRING, published_raw STRING, ingested_at STRING,
            kafka_ts TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (source)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER} (
            id STRING, source STRING, title STRING, body STRING,
            link STRING, published_at TIMESTAMP, ingested_at TIMESTAMP,
            dt DATE
        ) USING iceberg
        PARTITIONED BY (dt)
    """)


def upsert_bronze(batch_df, _):
    batch_df.createOrReplaceTempView("b")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {BRONZE} t USING b s ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT *
    """)


def upsert_silver(batch_df, _):
    clean = (
        batch_df
        .withColumn("body", F.regexp_replace(F.coalesce("summary", F.lit("")), "<[^>]+>", ""))
        .withColumn("body", F.trim(F.regexp_replace("body", "\\s+", " ")))
        .withColumn("published_at",
                    F.coalesce(F.to_timestamp("published_raw"), F.col("kafka_ts")))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("dt", F.to_date("published_at"))
        .filter(F.col("title").isNotNull() & (F.length("title") > 0))
        .select("id", "source", "title", "body", "link", "published_at", "ingested_at", "dt")
        .dropDuplicates(["id"])
    )
    clean.createOrReplaceTempView("s")
    clean.sparkSession.sql(f"""
        MERGE INTO {SILVER} t USING s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    events = (
        raw.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("e.*", "kafka_ts")
        .withWatermark("kafka_ts", "2 hours")
    )

    q_bronze = (
        events.writeStream.foreachBatch(upsert_bronze)
        .option("checkpointLocation", "/tmp/chk/bronze")
        .outputMode("append").start()
    )
    q_silver = (
        events.writeStream.foreachBatch(upsert_silver)
        .option("checkpointLocation", "/tmp/chk/silver")
        .outputMode("append").start()
    )

    print("[streaming] bronze/silver 스트리밍 시작 — Ctrl+C 로 종료")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
