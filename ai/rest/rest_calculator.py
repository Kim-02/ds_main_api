import math
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class WorkerRawInput:
    worker_id: str
    hr: float
    temp_c: float
    humid: float
    age: int
    gender: int
    height_cm: float
    weight_kg: float
    work_duration_min: int
    elderly_flag: int
    heart_disease: int
    hypertension: int
    other_disease: int
    baseline_hr: Optional[float] = None


class RestCalculator:
    @staticmethod
    def calc_heat_index_c(temp_c: float, rh: float) -> float:
        t1 = temp_c * math.atan(0.151977 * math.sqrt(rh + 8.313659))
        t2 = math.atan(temp_c + rh)
        t3 = math.atan(rh - 1.67633)
        t4 = 0.00391838 * (rh ** 1.5) * math.atan(0.023101 * rh)
        tw = t1 + t2 - t3 + t4 - 4.686035
        hi = (
            -0.2442
            + (0.55399 * tw)
            + (0.45535 * temp_c)
            - (0.0022 * tw ** 2)
            + (0.00278 * tw * temp_c)
            + 3.0
        )
        return round(float(hi), 2)

    @staticmethod
    def calc_risk_factor_count(
        elderly_flag: int,
        heart_disease: int,
        hypertension: int,
        other_disease: int,
    ) -> int:
        return (
            int(elderly_flag)
            + int(heart_disease)
            + int(hypertension)
            + int(other_disease)
        )

    @staticmethod
    def resolve_baseline_hr(
        current_hr: float,
        baseline_hr: Optional[float],
    ) -> float:
        if baseline_hr is None:
            return float(current_hr)
        return float(baseline_hr)

    @staticmethod
    def calc_hr_delta_from_baseline(
        current_hr: float,
        baseline_hr: float,
    ) -> float:
        return round(float(current_hr) - float(baseline_hr), 2)

    @classmethod
    def make_feature_dict(cls, raw: WorkerRawInput) -> Dict:
        baseline_hr = cls.resolve_baseline_hr(raw.hr, raw.baseline_hr)
        heat_index = cls.calc_heat_index_c(raw.temp_c, raw.humid)
        risk_factor_count = cls.calc_risk_factor_count(
            raw.elderly_flag,
            raw.heart_disease,
            raw.hypertension,
            raw.other_disease,
        )
        hr_delta = cls.calc_hr_delta_from_baseline(raw.hr, baseline_hr)

        return {
            "worker_id": raw.worker_id,
            "hr_30s_avg": float(raw.hr),
            "heat_index": float(heat_index),
            "hr_delta_from_baseline": float(hr_delta),
            "age": int(raw.age),
            "gender": int(raw.gender),
            "height_cm": float(raw.height_cm),
            "weight_kg": float(raw.weight_kg),
            "elderly_flag": int(raw.elderly_flag),
            "risk_factor_count": int(risk_factor_count),
            "work_duration_min": int(raw.work_duration_min),
            "baseline_hr": float(baseline_hr),
        }
