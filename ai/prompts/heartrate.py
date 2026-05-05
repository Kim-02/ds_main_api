def build_prompt(
    *,
    current_heartrate: int,
    threshold: int,
    worker_name: str,
    process_name: str,
    precautions: str,
    health_baseline: dict | None,
    recent_detections: list[dict],
    rest_recommendation: str | None = None,
) -> str:
    history_lines = []
    for item in recent_detections:
        objs = ", ".join(
            d["label"] for d in (item.get("detections") or {}).get("objects", [])
        ) or "없음"
        history_lines.append(f"  - {item['timestamp']}: 탐지 객체=[{objs}]")

    history_text = "\n".join(history_lines) if history_lines else "  - 이력 없음"
    baseline_text = f"- 개인 건강 기저치: {health_baseline}" if health_baseline else ""
    rest_text = f"\n[회귀 분석 휴식 권고]\n{rest_recommendation}" if rest_recommendation else ""

    return f"""당신은 산업 보건 전문가입니다. 작업자의 건강 이상 상황을 분석하고 조치 방안을 한국어로 답변하세요.

[현재 상황]
- 공정명: {process_name}
- 작업자: {worker_name}
- 현재 심박수: {current_heartrate} bpm (임계값: {threshold} bpm 초과)
{baseline_text}
- 공정 주의사항: {precautions or "없음"}
{rest_text}

[최근 CCTV 탐지 이력 (YOLO)]
{history_text}

[현재 CCTV 프레임]
첨부된 이미지를 분석하여 다음을 수행하세요:
1. 작업자의 현재 상태(자세, 움직임)를 파악하세요.
2. 주변 환경에서 심박 상승을 유발할 수 있는 요인을 탐색하세요.
3. 즉각적인 조치 방안을 제시하세요.
4. 휴식 권고 여부를 판단하세요.

답변 형식:
- 위험 수준: [낮음/중간/높음]
- 현장 상황: (2-3문장)
- 권고 조치: (번호 목록)
- 휴식 필요 여부: [예/아니오] (이유)
"""
