"""
임베딩 색인 잡: Iceberg silver_news → 임베딩 → Qdrant upsert
-----------------------------------------------------------
- 최근 N일(기본 3일) 기사만 대상으로 하여 비용/시간을 제한
- 배치 단위로 임베딩 호출
- upsert 이므로 중복 실행해도 안전(멱등)

실행:  python -m rag.embed          (airflow 컨테이너 내부)
        make embed
"""
import os
from datetime import date, timedelta

from pyiceberg.expressions import GreaterThanOrEqual

from rag import llm, store
from rag.iceberg_io import load_demo_catalog

LOOKBACK_DAYS = int(os.getenv("EMBED_LOOKBACK_DAYS", "3"))
BATCH = 64


def _point_id(article_id: str) -> int:
    # 16-hex 문자열 → uint64 (Qdrant point id 규격)
    return int(article_id, 16)


def main():
    cutoff = date.today() - timedelta(days=LOOKBACK_DAYS)
    catalog = load_demo_catalog()
    table = catalog.load_table("news.silver_news")

    rows = (
        table.scan(row_filter=GreaterThanOrEqual("dt", cutoff.isoformat()))
        .to_arrow()
        .to_pylist()
    )
    if not rows:
        print("[embed] 색인할 신규 기사 없음")
        return

    c = store.client()
    store.ensure_collection(c)

    total = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        texts = [f"{r['title']}\n{r.get('body') or ''}".strip() for r in chunk]
        vectors = llm.embed(texts)
        ids = [_point_id(r["id"]) for r in chunk]
        payloads = [{
            "article_id": r["id"],
            "source": r["source"],
            "title": r["title"],
            "body": (r.get("body") or "")[:2000],
            "link": r["link"],
            "published_at": str(r.get("published_at")),
        } for r in chunk]
        store.upsert(c, ids, vectors, payloads)
        total += len(chunk)

    mode = "MOCK" if llm.is_mock() else "OpenAI"
    print(f"[embed] {total}건 색인 완료 (임베딩={mode}, 최근 {LOOKBACK_DAYS}일)")


if __name__ == "__main__":
    main()
