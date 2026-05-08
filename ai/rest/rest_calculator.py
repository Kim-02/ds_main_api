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
        print(f"[RestCalculator] START calc_heat_index_c temp_c={temp_c}, rh={rh}")
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
        result = round(float(hi), 2)
        print(
            "[RestCalculator] END calc_heat_index_c "
            f"tw={round(float(tw), 4)}, heat_index={result}"
        )
        return result

    @staticmethod
    def calc_risk_factor_count(
        elderly_flag: int,
        heart_disease: int,
        hypertension: int,
        other_disease: int,
    ) -> int:
        print(
            "[RestCalculator] START calc_risk_factor_count "
            f"elderly_flag={elderly_flag}, heart_disease={heart_disease}, "
            f"hypertension={hypertension}, other_disease={other_disease}"
        )
        result = (
            int(elderly_flag)
            + int(heart_disease)
            + int(hypertension)
            + int(other_disease)
        )
        print(f"[RestCalculator] END calc_risk_factor_count result={result}")
        return result

    @staticmethod
    def resolve_baseline_hr(
        current_hr: float,
        baseline_hr: Optional[float],
    ) -> float:
        print(
            "[RestCalculator] START resolve_baseline_hr "
            f"current_hr={current_hr}, baseline_hr={baseline_hr}"
        )
        if baseline_hr is None:
            result = float(current_hr)
            print(f"[RestCalculator] END resolve_baseline_hr source=current_hr result={result}")
            return result
        result = float(baseline_hr)
        print(f"[RestCalculator] END resolve_baseline_hr source=baseline_hr result={result}")
        return result

    @staticmethod
    def calc_hr_delta_from_baseline(
        current_hr: float,
        baseline_hr: float,
    ) -> float:
        print(
            "[RestCalculator] START calc_hr_delta_from_baseline "
            f"current_hr={current_hr}, baseline_hr={baseline_hr}"
        )
        result = round(float(current_hr) - float(baseline_hr), 2)
        print(f"[RestCalculator] END calc_hr_delta_from_baseline result={result}")
        return result

    @classmethod
    def make_feature_dict(cls, raw: WorkerRawInput) -> Dict:
        print(f"[RestCalculator] START make_feature_dict raw={raw}")
        baseline_hr = cls.resolve_baseline_hr(raw.hr, raw.baseline_hr)
        heat_index = cls.calc_heat_index_c(raw.temp_c, raw.humid)
        risk_factor_count = cls.calc_risk_factor_count(
            raw.elderly_flag,
            raw.heart_disease,
            raw.hypertension,
            raw.other_disease,
        )
        hr_delta = cls.calc_hr_delta_from_baseline(raw.hr, baseline_hr)

        feature_dict = {
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
        print(f"[RestCalculator] END make_feature_dict feature_dict={feature_dict}")
        return feature_dict
