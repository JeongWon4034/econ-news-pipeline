#!/usr/bin/env bash
# silver → gold 집계 배치 실행. 인자로 날짜(YYYY-MM-DD)를 주면 해당 일자만 집계.
set -euo pipefail
exec /opt/spark/bin/spark-submit --master "local[*]" \
  /app/batch/gold_aggregation.py "${1:-}"
