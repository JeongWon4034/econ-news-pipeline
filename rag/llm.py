"""
LLM / 임베딩 클라이언트 (얇은 래퍼)
-----------------------------------
OPENAI_API_KEY 가 설정돼 있으면 실제 API를 호출하고,
비어 있으면 '오프라인 mock 모드'로 동작한다.
→ API 키 없이도 파이프라인 전체를 데모/테스트할 수 있게 한다.
"""
import hashlib
import os

_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
_LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
_EMB_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBED_DIM = 1536  # text-embedding-3-small 차원

_client = None
if _API_KEY:
    try:
        from openai import OpenAI
        _client = OpenAI(api_key=_API_KEY, base_url=_BASE_URL)
    except Exception as e:  # 라이브러리 미설치 등 → mock 으로 폴백
        print(f"[llm] OpenAI 초기화 실패, mock 모드로 전환: {e}")


def is_mock() -> bool:
    return _client is None


def _mock_embedding(text: str) -> list[float]:
    """텍스트 해시 기반 결정적 유사 임베딩 (외부 호출 없이 검색 데모 가능)."""
    h = hashlib.sha256(text.encode("utf-8")).digest()
    raw = (h * ((EMBED_DIM // len(h)) + 1))[:EMBED_DIM]
    vec = [(b / 255.0) - 0.5 for b in raw]
    norm = sum(v * v for v in vec) ** 0.5 or 1.0
    return [v / norm for v in vec]


def embed(texts: list[str]) -> list[list[float]]:
    if is_mock():
        return [_mock_embedding(t) for t in texts]
    resp = _client.embeddings.create(model=_EMB_MODEL, input=texts)
    return [d.embedding for d in resp.data]


def chat(system: str, user: str, temperature: float = 0.3) -> str:
    if is_mock():
        # mock: 입력을 요약 흉내내어 반환 (구조 확인용)
        preview = user.strip().replace("\n", " ")[:400]
        return f"[MOCK-LLM] ({system[:24]}...) 응답:\n{preview}"
    resp = _client.chat.completions.create(
        model=_LLM_MODEL,
        temperature=temperature,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
    )
    return resp.choices[0].message.content.strip()
