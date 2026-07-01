"""
일일 경제 브리핑 DAG
--------------------
매일 아침 06:00(Asia/Seoul)에 실행:

  gold_aggregation → embed_articles → generate_briefing

- gold_aggregation : silver → gold 집계 (spark 컨테이너에 위임)
- embed_articles   : 최근 기사 임베딩 → Qdrant 색인
- generate_briefing: 멀티에이전트가 '오늘의 경제 브리핑' 생성

스트리밍(Kafka→Spark→Iceberg)은 상시 가동되므로 DAG에 포함하지 않는다.
"""
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

KST = pendulum.timezone("Asia/Seoul")

default_args = {"retries": 1}

with DAG(
    dag_id="daily_economic_briefing",
    description="경제 뉴스 gold 집계 → 임베딩 → 멀티에이전트 브리핑",
    schedule="0 6 * * *",          # 매일 06:00 KST
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    catchup=False,
    default_args=default_args,
    tags=["news", "rag", "agents"],
) as dag:

    # 1) gold 집계: spark 컨테이너에서 실행 (docker exec)
    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command='docker exec spark bash /app/batch/run_gold.sh "{{ ds }}"',
    )

    # 2) 임베딩 색인: airflow 워커에서 직접 실행
    embed_articles = BashOperator(
        task_id="embed_articles",
        bash_command="cd /opt/project && python -m rag.embed",
    )

    # 3) 멀티에이전트 브리핑 생성
    generate_briefing = BashOperator(
        task_id="generate_briefing",
        bash_command="cd /opt/project && python -m agents.run_briefing",
    )

    gold_aggregation >> embed_articles >> generate_briefing
