def build_prompt(
    *,
    detected_labels: list[str],
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

    return f"""당신은 산업 재난 대응 전문가입니다. CCTV에서 감지된 위험 상황을 분석하고 대응 방안을 한국어로 답변하세요.

[현재 감지 상황]
- 공정명: {process_name}
- 감지된 위험 요소: {", ".join(detected_labels)}
- 공정 주의사항 (진압 솔루션): {precautions or "없음"}

[최근 CCTV 탐지 이력 (YOLO)]
{history_text}

[현재 CCTV 프레임]
첨부된 이미지를 분석하여 다음을 수행하세요:
1. 재난 발생 원인을 파악하고 공정 주의사항 기반 진압 솔루션을 제시하세요.
2. 현장의 인명 피해 위험도를 평가하세요.
3. 즉각 대피가 필요한지 판단하세요.
4. 재난 확산 방지를 위한 긴급 조치 3가지를 제시하세요.

답변 형식:
- 위험 수준: [경고/위험/긴급]
- 재난 원인 추정: (1-2문장)
- 진압/대응 방안: (번호 목록)
- 즉시 대피 필요 여부: [예/아니오]
"""
