#!/usr/bin/env bash
# 스트리밍 잡 엔트리포인트. Iceberg REST 카탈로그가 뜰 때까지 기다린 뒤 spark-submit.
set -euo pipefail

echo "[spark] Iceberg REST 카탈로그 대기..."
until curl -sf "${ICEBERG_REST_URI:-http://iceberg-rest:8181}/v1/config" >/dev/null; do
  sleep 3
done
echo "[spark] 카탈로그 준비 완료. 스트리밍 잡 제출."

exec /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /app/streaming/spark_streaming.py
