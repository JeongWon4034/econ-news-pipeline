"""
RAG Retriever: 질의 → 임베딩 → Qdrant 검색 → 관련 기사 컨텍스트 반환
멀티에이전트의 Researcher가 이 모듈을 호출한다.
CLI 로 단독 질의도 가능:  python -m rag.retriever "최근 금리 관련 뉴스"
"""
import sys

from rag import llm, store


def retrieve(query: str, top_k: int = 5) -> list[dict]:
    c = store.client()
    store.ensure_collection(c)
    qvec = llm.embed([query])[0]
    return store.search(c, qvec, top_k=top_k)


def format_contexts(hits: list[dict]) -> str:
    lines = []
    for i, h in enumerate(hits, 1):
        lines.append(
            f"[{i}] ({h.get('source')}) {h.get('title')}\n"
            f"    {h.get('body', '')[:200]}\n"
            f"    링크: {h.get('link')}"
        )
    return "\n".join(lines) if lines else "(검색 결과 없음)"


if __name__ == "__main__":
    q = sys.argv[1] if len(sys.argv) > 1 else "오늘 주요 경제 이슈"
    print(format_contexts(retrieve(q)))
