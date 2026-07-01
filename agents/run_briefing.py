"""
'오늘의 경제 브리핑' 생성 엔트리포인트.
멀티에이전트 그래프를 실행하고 결과를 파일과 콘솔에 출력한다.

실행:  python -m agents.run_briefing        (airflow 컨테이너)
        make brief
"""
import os
import sys
from datetime import date

from agents.graph import run

OUT_DIR = os.getenv("BRIEFING_DIR", "/opt/project/docs/briefings")


def main(query: str | None):
    os.makedirs(OUT_DIR, exist_ok=True)
    state = run(query or "오늘 한국 경제의 주요 이슈")
    briefing = state["briefing"]

    out_path = os.path.join(OUT_DIR, f"briefing_{date.today().isoformat()}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(briefing)

    print("=" * 60)
    print(f"토론 루프: {state['loops']}회  |  최종 판정: {state['verdict']}")
    print(f"저장 위치: {out_path}")
    print("=" * 60)
    print(briefing)
    return out_path


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
