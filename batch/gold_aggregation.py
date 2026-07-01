"""
배치 집계: silver_news → gold 테이블
------------------------------------
Airflow가 하루 한 번(또는 수동 make gold) 호출한다.

- gold_daily_source : 일자·언론사별 기사 수
- gold_daily_keyword: 일자별 상위 키워드 빈도 (제목 기반 간이 토큰화)

멀티에이전트/대시보드가 "오늘 무엇이 많이 언급됐나"를 빠르게 조회하는 서빙 레이어.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "demo"
DB = "news"
SILVER = f"{CATALOG}.{DB}.silver_news"

# 키워드 집계에서 제외할 흔한 불용어(간이)
STOPWORDS = {
    "기자", "종합", "속보", "단독", "그", "이", "및", "등", "위해", "관련", "대한",
    "오늘", "지난", "올해", "경제", "뉴스", "코스피", "코스닥",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("news-gold-batch")
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


def main(target_date: str | None):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.table(SILVER)
    if target_date:
        silver = silver.filter(F.col("dt") == F.lit(target_date))

    # 1) 일자·언론사별 기사 수
    daily_source = (
        silver.groupBy("dt", "source")
        .agg(F.count("*").alias("article_count"))
    )
    (daily_source.writeTo(f"{CATALOG}.{DB}.gold_daily_source")
        .using("iceberg").partitionedBy("dt").createOrReplace())

    # 2) 일자별 상위 키워드
    tokens = (
        silver.select("dt", F.explode(F.split(F.col("title"), "\\s+")).alias("token"))
        .withColumn("token", F.regexp_replace("token", "[^가-힣A-Za-z0-9]", ""))
        .filter(F.length("token") >= 2)
    )
    stop_b = spark.sparkContext.broadcast(STOPWORDS)
    daily_keyword = (
        tokens.filter(~F.col("token").isin(list(stop_b.value)))
        .groupBy("dt", "token")
        .agg(F.count("*").alias("freq"))
    )
    (daily_keyword.writeTo(f"{CATALOG}.{DB}.gold_daily_keyword")
        .using("iceberg").partitionedBy("dt").createOrReplace())

    print("[gold] 집계 완료")
    daily_source.orderBy(F.desc("article_count")).show(10, truncate=False)
    (daily_keyword.orderBy(F.desc("freq")).show(15, truncate=False))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
