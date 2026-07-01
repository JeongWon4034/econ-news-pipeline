"""
경제 뉴스 RSS Producer
----------------------
여러 언론사의 경제 RSS 피드를 주기적으로 폴링하여, 새로 올라온 기사를
Kafka 토픽(news.raw)으로 발행한다.

- 이미 발행한 기사는 URL 해시로 중복 방지 (프로세스 로컬 캐시 + Kafka key)
- 발행 실패 시 재시도, 피드 하나가 죽어도 전체는 계속 동작
- Kafka key = 기사 URL 해시  → 파티션 분산 + 다운스트림 멱등 처리에 활용

환경변수: KAFKA_BOOTSTRAP, KAFKA_TOPIC, POLL_INTERVAL_SEC
"""
import hashlib
import json
import os
import time
from datetime import datetime, timezone

import feedparser
import yaml
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "news.raw")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SEC", "60"))
FEEDS_PATH = os.getenv("FEEDS_PATH", "/app/feeds.yaml")


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def load_feeds(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["feeds"]


def connect_producer() -> KafkaProducer:
    """Kafka가 뜰 때까지 재시도하며 프로듀서 생성."""
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=200,
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka 대기 중... ({attempt + 1}/30)")
            time.sleep(5)
    raise RuntimeError("Kafka 브로커에 연결하지 못했습니다.")


def to_event(entry, source: str) -> dict:
    """feedparser 엔트리를 표준 이벤트 스키마로 변환."""
    link = entry.get("link", "")
    published = entry.get("published", "") or entry.get("updated", "")
    summary = entry.get("summary", "") or entry.get("description", "")
    return {
        "id": url_hash(link),
        "source": source,
        "title": entry.get("title", "").strip(),
        "summary": summary.strip(),
        "link": link,
        "published_raw": published,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    feeds = load_feeds(FEEDS_PATH)
    producer = connect_producer()
    seen: set[str] = set()  # 프로세스 내 중복 방지 캐시
    print(f"[producer] 시작 — {len(feeds)}개 피드, 토픽={TOPIC}, 주기={POLL_INTERVAL}s")

    while True:
        published_count = 0
        for feed in feeds:
            source, url = feed["name"], feed["url"]
            try:
                parsed = feedparser.parse(url)
                for entry in parsed.entries:
                    event = to_event(entry, source)
                    if not event["link"] or event["id"] in seen:
                        continue
                    producer.send(TOPIC, key=event["id"], value=event)
                    seen.add(event["id"])
                    published_count += 1
            except Exception as e:  # 피드 하나가 죽어도 전체는 계속
                print(f"[producer] '{source}' 폴링 실패: {e}")
        producer.flush()
        print(f"[producer] {datetime.now().strftime('%H:%M:%S')} — 신규 {published_count}건 발행 "
              f"(누적 {len(seen)}건)")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
