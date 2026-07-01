# 경제 뉴스 실시간 데이터 파이프라인 + 멀티에이전트 분석

경제 뉴스를 **실시간으로 수집 → 정제 → 레이크하우스 적재**하고, **RAG + 멀티에이전트 AI**가
매일 아침 "오늘의 경제 브리핑"을 자동 생성하는 엔드투엔드 데이터 플랫폼입니다.
인프라 전체를 **Docker Compose 하나로** 기동합니다.

> 이 프로젝트의 목표는 특정 도메인 해결이 아니라, **현재 데이터 엔지니어링 트렌드 스택
> (Kafka · Spark · Iceberg 레이크하우스 · Airflow · RAG · 멀티에이전트)을 하나의 작동하는
> 시스템으로 직접 구성·운영**해 보는 것입니다. 데이터가 쉽게 확보되는 경제 뉴스를 소재로 골랐습니다.

---

## 아키텍처

```
 [경제뉴스 RSS]        (1)                  (2)
  연합/한겨레/매경 ──► Producer ──► Kafka ──► Spark Structured Streaming
   주기적 폴링          (Python)   news.raw    정제·중복제거·본문파싱
                                                      │
                                                      ▼
                                    (3) Iceberg on MinIO (레이크하우스)
                                        bronze ─► silver ─► gold
                                                      │
                        ┌──────────────────────────────┤
                        ▼                              ▼
            (4) 임베딩 → Qdrant              (5) Airflow DAG (매일 06:00)
                        │                    gold 집계 → 임베딩 → 브리핑
                        ▼
            (6) RAG + LangGraph 멀티에이전트
                Researcher → Analyst → Critic → Reporter
                        │
                        ▼
                "오늘의 경제 브리핑" 자동 생성
```

자세한 데이터 모델과 설계 결정은 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) 참고.

---

## 기술 스택

| 레이어 | 기술 | 선택 이유 |
|---|---|---|
| 수집 | **Apache Kafka** (KRaft) | 실시간 수집 표준. 수집/처리 디커플링 |
| 처리 | **Spark Structured Streaming** | 배치+스트리밍 통합, 마이크로배치 MERGE 멱등 적재 |
| 스토리지 | **MinIO** (S3 호환) | HDFS의 현대적 대체 |
| 테이블 포맷 | **Apache Iceberg** (REST Catalog) | ACID·스키마 진화·타임트래블 |
| 서빙/RAG | **Qdrant** | 경량 벡터 DB |
| 오케스트레이션 | **Airflow** | DAG·스케줄·재시도 |
| AI | **LangGraph** 멀티에이전트 + RAG | 역할 분담 + 토론 루프로 브리핑 생성 |
| 인프라 | **Docker Compose** | 전체 스택 코드화·재현 |

> **왜 Hadoop/HDFS를 안 썼나?** 로컬에서도 클라우드 오브젝트 스토리지 환경을 그대로 재현하고,
> Iceberg 레이크하우스(ACID·타임트래블)의 이점을 살리기 위해 MinIO + Iceberg를 선택했습니다.

---

## 빠른 시작

### 사전 요구
- Docker / Docker Compose
- (선택) `OPENAI_API_KEY` — 없으면 **오프라인 mock 모드**로 임베딩·LLM이 동작해 전체 흐름을 확인할 수 있습니다.

### 실행
```bash
cp .env.example .env      # 필요 시 OPENAI_API_KEY 입력
make up                   # 전체 스택 기동 (docker compose up -d --build)
make ps                   # 상태 확인
```

### 접속
| 서비스 | URL | 계정 |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO 콘솔 | http://localhost:9001 | minioadmin / minioadmin |
| Qdrant | http://localhost:6333/dashboard | — |

### 데이터가 흐르는지 확인
```bash
make topic     # Kafka news.raw 토픽으로 뉴스가 들어오는지 실시간 확인
make gold      # silver → gold 집계 실행 후 상위 키워드 출력
make embed     # 최근 기사 임베딩 → Qdrant 색인
make brief     # 멀티에이전트 '오늘의 경제 브리핑' 즉시 생성
```

생성된 브리핑은 `docs/briefings/briefing_YYYY-MM-DD.md` 에 저장됩니다.

---

## 프로젝트 구조

```
econ-news-pipeline/
├── docker-compose.yml       # 전체 인프라 정의
├── Makefile                 # 자주 쓰는 명령 (make help)
├── .env.example
├── producer/                # (1) RSS 폴링 → Kafka
│   ├── rss_producer.py
│   └── feeds.yaml           # 언론사 RSS 목록
├── streaming/               # (2)(3) Kafka → 정제 → Iceberg
│   ├── spark_streaming.py
│   ├── entrypoint.sh
│   └── Dockerfile           # Spark + Iceberg + Kafka 커넥터
├── batch/                   # gold 집계 배치
│   └── gold_aggregation.py
├── rag/                     # (4) 임베딩·검색
│   ├── embed.py  retriever.py  store.py  llm.py  iceberg_io.py
├── agents/                  # (6) LangGraph 멀티에이전트
│   ├── graph.py  roles.py  run_briefing.py
├── airflow/                 # (5) 오케스트레이션
│   ├── Dockerfile
│   └── dags/daily_briefing.py
└── docs/
    └── ARCHITECTURE.md
```

---

## 데이터 모델 (Iceberg)

| 테이블 | 레이어 | 내용 | 파티션 |
|---|---|---|---|
| `demo.news.bronze_news` | Bronze | 원천 이벤트(감사·재처리용) | `source` |
| `demo.news.silver_news` | Silver | 정제(HTML 제거·시각파싱·중복제거) | `dt` |
| `demo.news.gold_daily_source` | Gold | 일자·언론사별 기사 수 | `dt` |
| `demo.news.gold_daily_keyword` | Gold | 일자별 상위 키워드 빈도 | `dt` |

---

## 멀티에이전트 동작 방식

```
Researcher ──검색(RAG)──► 핵심 이슈 후보 정리
     │
Analyst ──분석──► 이슈별 의미·영향 해석
     │
Critic ──검증──► 근거 부족/과장 지적
     │   └─ REVISE → Researcher로 되돌림 (최대 AGENT_MAX_LOOPS회)
     │   └─ APPROVE ↓
Reporter ──작성──► 최종 '오늘의 경제 브리핑'
```

`Critic ↔ Researcher` 루프가 **토론을 통한 품질 개선(할루시네이션 억제)** 의 핵심이며,
`AGENT_MAX_LOOPS`(기본 2)로 무한 루프와 비용 폭발을 방지합니다.

---

## 운영 노트 / 회고

- **버전 호환이 가장 큰 허들**이었음: Spark 3.5 ↔ Iceberg 1.5 ↔ AWS 번들 조합을 고정하고,
  커넥터 JAR을 이미지에 미리 내려받아 기동 안정성을 확보.
- **멱등 적재**: 스트리밍은 `foreachBatch` + `MERGE INTO ... ON id`로 재처리에도 중복이 없도록 설계.
- **오프라인 동작**: API 키 없이도 데모가 되도록 LLM/임베딩에 mock 폴백을 넣어 온보딩 장벽을 낮춤.

### 향후 개선 (Out of scope for v1)
- Kafka Schema Registry / Debezium CDC 연계
- 실시간 대시보드(Streamlit/Superset)
- Kubernetes 배포, 멀티노드 Spark
- 브리핑 품질 평가(LLM-as-judge) 자동화
