"""회귀 분석 기반 휴식 권고 모델."""
import logging
from pathlib import Path

import joblib
import numpy as np

logger = logging.getLogger(__name__)

_MODEL_PATH = Path("models/rest_recommendation.pkl")
_model = None


def _load_model():
    global _model
    if _model is not None:
        return _model
    if _MODEL_PATH.exists():
        _model = joblib.load(_MODEL_PATH)
        logger.info("Rest recommendation model loaded")
    return _model


def predict_rest_needed(
    heartrate: int,
    temperature: float,
    humidity: float,
    health_baseline: dict | None,
) -> str | None:
    model = _load_model()
    resting_hr = (health_baseline or {}).get("resting_heartrate", 70)
    hr_delta = heartrate - resting_hr

    if model is not None:
        try:
            features = np.array([[heartrate, temperature, humidity, resting_hr]])
            prob = model.predict_proba(features)[0][1]
            if prob > 0.7:
                return f"회귀 모델 예측: 휴식 필요 확률 {prob:.0%} (심박 증가량={hr_delta}bpm)"
            return None
        except Exception:
            logger.exception("Regression model prediction failed, using rule-based fallback")

    # 규칙 기반 폴백
    if hr_delta > 40 or (heartrate > 130 and temperature > 30):
        return f"심박 증가량 {hr_delta}bpm, 환경 온도 {temperature}°C — 즉시 휴식 권고"
    if hr_delta > 25:
        return f"심박 증가량 {hr_delta}bpm — 단기 휴식 권고"
    return None
