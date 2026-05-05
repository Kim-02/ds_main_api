def build_prompt(
    *,
    current_temperature: float,
    current_humidity: float,
    threshold: float,
    process_name: str,
    precautions: str,
    recent_detections: list[dict],
) -> str:
    history_lines = []
    for item in recent_detections:
        objs = ", ".join(
            d["label"] for d in (item.get("detections") or {}).get("objects", [])
        ) or "없음"
        history_lines.append(f"  - {item['timestamp']}: 탐지 객체=[{objs}]")

    history_text = "\n".join(history_lines) if history_lines else "  - 이력 없음"

    return f"""당신은 산업 안전 전문가입니다. 다음 상황을 분석하고 조치 방안을 한국어로 답변하세요.

[현재 상황]
- 공정명: {process_name}
- 현재 온도: {current_temperature}°C (임계값: {threshold}°C 초과)
- 현재 습도: {current_humidity}%
- 공정 주의사항: {precautions or "없음"}

[최근 CCTV 탐지 이력 (YOLO)]
{history_text}

[현재 CCTV 프레임]
첨부된 이미지를 분석하여 다음을 수행하세요:
1. 이미지에서 사람의 움직임이나 위험 징후를 탐색하세요.
2. 온도 이상의 원인이 될 수 있는 요소를 파악하세요.
3. 즉각적인 조치 방안을 3가지 이내로 제시하세요.
4. 위험 수준을 [낮음/중간/높음] 중 하나로 평가하세요.

답변 형식:
- 위험 수준: [낮음/중간/높음]
- 현장 상황: (2-3문장)
- 권고 조치: (번호 목록)
"""
