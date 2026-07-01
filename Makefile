# 경제 뉴스 파이프라인 — 자주 쓰는 명령 모음
# `make help` 로 목록 확인

.DEFAULT_GOAL := help
.PHONY: help env up down logs ps topic gold embed brief clean

help: ## 명령 목록
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

env: ## .env 파일 생성 (.env.example 복사)
	@test -f .env || (cp .env.example .env && echo "✅ .env 생성됨")

up: env ## 전체 스택 기동
	docker compose up -d --build

down: ## 전체 스택 종료
	docker compose down

logs: ## 로그 따라가기 (예: make logs S=spark)
	docker compose logs -f $(S)

ps: ## 컨테이너 상태
	docker compose ps

topic: ## news.raw 토픽 실시간 확인
	docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic news.raw --from-beginning

gold: ## silver → gold 집계 배치 실행
	docker exec -it spark bash /app/batch/run_gold.sh

embed: ## 최신 기사 임베딩 → Qdrant 색인
	docker exec -it airflow-scheduler python /opt/project/rag/embed.py

brief: ## 멀티에이전트 "오늘의 경제 브리핑" 즉시 생성
	docker exec -it airflow-scheduler python /opt/project/agents/run_briefing.py

clean: ## 스택 + 볼륨 완전 삭제 (데이터 초기화)
	docker compose down -v
