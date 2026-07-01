"""Qdrant 벡터 스토어 헬퍼 — 컬렉션 보장 / upsert / 검색."""
import os

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

from rag.llm import EMBED_DIM

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "news_articles")


def client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL)


def ensure_collection(c: QdrantClient):
    existing = {col.name for col in c.get_collections().collections}
    if COLLECTION not in existing:
        c.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
        )
        print(f"[qdrant] 컬렉션 생성: {COLLECTION}")


def upsert(c: QdrantClient, ids, vectors, payloads):
    points = [
        PointStruct(id=i, vector=v, payload=p)
        for i, v, p in zip(ids, vectors, payloads)
    ]
    c.upsert(collection_name=COLLECTION, points=points)


def search(c: QdrantClient, vector, top_k: int = 5):
    hits = c.search(collection_name=COLLECTION, query_vector=vector, limit=top_k)
    return [{"score": h.score, **(h.payload or {})} for h in hits]
