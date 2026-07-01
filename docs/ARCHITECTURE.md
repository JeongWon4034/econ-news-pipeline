# 아키텍처 & 설계 결정

## 1. 데이터 플로우 (엔드투엔드)

1. **Producer** (`producer/rss_producer.py`)
   여러 경제 RSS를 `POLL_INTERVAL_SEC`마다 폴링 → 기사 URL 해시를 key로 Kafka `news.raw` 발행.
   프로세스 로컬 캐시로 재발행을 줄이고, 다운스트림은 `id`로 최종 멱등 처리.

2. **Spark Structured Streaming** (`streaming/spark_streaming.py`)
   `news.raw` 구독 → `from_json`으로 파싱 → 2개 스트림 쿼리:
   - **bronze**: 원천 이벤트 append(MERGE NOT MATCHED) — 감사/재처리
   - **silver**: HTML 태그 제거·공백 정리·발행시각 파싱·`dt` 계산 → `MERGE ... ON id`로 upsert
   `withWatermark("kafka_ts","2 hours")`로 지연/중복 이벤트를 흡수.

3. **Iceberg REST Catalog + MinIO**
   카탈로그는 `tabulario/iceberg-rest`, 스토리지는 MinIO(`s3://warehouse/`).
   Spark는 `SparkCatalog(type=rest)` + `S3FileIO`로 접속.

4. **Gold 배치** (`batch/gold_aggregation.py`)
   silver를 읽어 일자·언론사별 기사 수, 일자별 키워드 빈도를 `createOrReplace`로 재생성.

5. **RAG 색인** (`rag/embed.py`)
   PyIceberg로 silver의 최근 N일 기사를 읽어 임베딩 → Qdrant upsert.
   포인트 id는 기사 16-hex id를 uint64로 변환해 사용(멱등).

6. **멀티에이전트** (`agents/graph.py`)
   LangGraph 상태 그래프. Researcher(RAG 검색)→Analyst→Critic→(조건분기)→Reporter.

7. **Airflow** (`airflow/dags/daily_briefing.py`)
   매일 06:00 KST: `gold_aggregation → embed_articles → generate_briefing`.

## 2. 주요 설계 결정 (Trade-offs)

### HDFS 대신 MinIO + Iceberg
로컬에서도 S3 기반 클라우드 환경을 재현하고, Iceberg의 ACID·스키마 진화·타임트래블을 얻기 위함.
Iceberg 카탈로그는 Hadoop 카탈로그(S3에서 원자적 rename 문제) 대신 **REST Catalog**를 사용.

### 스트리밍 멱등성
`foreachBatch` + `MERGE INTO ... ON id`. at-least-once 전달 + 재처리 상황에서도 중복 없음.

### gold 배치를 Airflow가 Spark에 위임
Airflow 워커에는 Spark가 없으므로, docker 소켓을 마운트해 `docker exec spark ...`로 실행.
(운영 환경에서는 SparkKubernetesOperator / Livy 등으로 대체 권장 — v1은 단순화)

### 오프라인 mock 모드
`OPENAI_API_KEY`가 없으면 결정적 해시 임베딩 + 템플릿 LLM 응답으로 폴백.
키 없이도 전체 파이프라인의 구조와 흐름을 검증할 수 있어 온보딩/CI에 유리.

## 3. 확장 포인트
- Producer에 본문 크롤링 단계 추가(요약만이 아닌 전문 임베딩)
- Kafka Schema Registry로 스키마 계약 강제
- Critic 판정에 근거 문장 인용 강제(정합성 향상)
- gold에 시계열 추세/이상탐지 테이블 추가
