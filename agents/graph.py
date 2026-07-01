"""
멀티에이전트 그래프 (LangGraph)
--------------------------------
Researcher → Analyst → Critic ──(REVISE)──► Researcher (재조사, 최대 N회)
                          └────(APPROVE)──► Reporter → 최종 브리핑

Critic ↔ Researcher 루프가 "토론을 통한 최적해 도출"의 핵심.
MAX_LOOPS 로 무한루프/비용 폭발을 방지한다.
"""
import os
from typing import TypedDict

from langgraph.graph import StateGraph, START, END

from rag import retriever
from rag.llm import chat
from agents import roles

MAX_LOOPS = int(os.getenv("AGENT_MAX_LOOPS", "2"))


class BriefingState(TypedDict):
    query: str
    contexts: str
    research: str
    analysis: str
    critique: str
    verdict: str
    loops: int
    briefing: str


def researcher(state: BriefingState) -> dict:
    # 재조사 시 검토 의견을 반영해 쿼리를 보강
    query = state["query"]
    if state.get("critique"):
        query = f"{query} (보강 포인트: {state['critique'][:200]})"
    hits = retriever.retrieve(query, top_k=6)
    contexts = retriever.format_contexts(hits)
    research = chat(roles.RESEARCHER, f"검색 결과:\n{contexts}")
    return {"contexts": contexts, "research": research, "loops": state.get("loops", 0) + 1}


def analyst(state: BriefingState) -> dict:
    prompt = f"[정리된 이슈]\n{state['research']}\n\n[원본 기사]\n{state['contexts']}"
    return {"analysis": chat(roles.ANALYST, prompt)}


def critic(state: BriefingState) -> dict:
    prompt = f"[분석]\n{state['analysis']}\n\n[원본 기사]\n{state['contexts']}"
    out = chat(roles.CRITIC, prompt)
    verdict = "APPROVE" if "APPROVE" in out.upper().split("VERDICT:")[-1] else "REVISE"
    # 루프 상한 도달 시 강제 승인하여 마무리
    if state.get("loops", 0) >= MAX_LOOPS:
        verdict = "APPROVE"
    return {"critique": out, "verdict": verdict}


def reporter(state: BriefingState) -> dict:
    prompt = f"[승인된 분석]\n{state['analysis']}\n\n[근거 기사]\n{state['contexts']}"
    return {"briefing": chat(roles.REPORTER, prompt, temperature=0.5)}


def route_after_critic(state: BriefingState) -> str:
    return "reporter" if state["verdict"] == "APPROVE" else "researcher"


def build_graph():
    g = StateGraph(BriefingState)
    g.add_node("researcher", researcher)
    g.add_node("analyst", analyst)
    g.add_node("critic", critic)
    g.add_node("reporter", reporter)

    g.add_edge(START, "researcher")
    g.add_edge("researcher", "analyst")
    g.add_edge("analyst", "critic")
    g.add_conditional_edges("critic", route_after_critic,
                            {"researcher": "researcher", "reporter": "reporter"})
    g.add_edge("reporter", END)
    return g.compile()


def run(query: str = "오늘 한국 경제의 주요 이슈") -> BriefingState:
    graph = build_graph()
    return graph.invoke({"query": query, "loops": 0})
