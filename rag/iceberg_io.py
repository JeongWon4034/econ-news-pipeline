"""PyIceberg로 Iceberg REST 카탈로그에 접속 (Spark 없이 파이썬에서 읽기)."""
import os

from pyiceberg.catalog import load_catalog


def load_demo_catalog():
    return load_catalog(
        "demo",
        **{
            "type": "rest",
            "uri": os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.path-style-access": "true",
        },
    )
