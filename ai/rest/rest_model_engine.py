from typing import Dict, List

import joblib
import pandas as pd

from .rest_calculator import RestCalculator, WorkerRawInput


FINAL_FORCE_REST = "반드시 휴식"
FINAL_STRONG_REST = "강한휴식권고"
FINAL_WEAK_REST = "약한휴식권고"
FINAL_NO_REST = "미휴식"

DEFAULT_FORCED_REST_WORK_MIN = 120


class RestModelEngine:
    def __init__(
        self,
        model_path: str,
        forced_rest_work_min: int = DEFAULT_FORCED_REST_WORK_MIN,
    ):
        payload = joblib.load(model_path)
        self.model = payload["model"]
        self.feature_cols: List[str] = payload["feature_cols"]
        self.label_name_map: Dict[int, str] = payload["label_name_map"]
        self.forced_rest_work_min = forced_rest_work_min
        self.worker_baseline_map: Dict[str, float] = {}

    def reset_worker(self, worker_id: str) -> None:
        if worker_id in self.worker_baseline_map:
            del self.worker_baseline_map[worker_id]

    def _inject_baseline(self, raw: WorkerRawInput) -> WorkerRawInput:
        if raw.baseline_hr is not None:
            self.worker_baseline_map[raw.worker_id] = float(raw.baseline_hr)
            return raw

        if raw.worker_id not in self.worker_baseline_map:
            self.worker_baseline_map[raw.worker_id] = float(raw.hr)

        raw.baseline_hr = self.worker_baseline_map[raw.worker_id]
        return raw

    def _check_force_rest(self, work_duration_min: int) -> bool:
        return int(work_duration_min) >= int(self.forced_rest_work_min)

    def predict(self, raw: WorkerRawInput) -> Dict:
        raw = self._inject_baseline(raw)
        feature_dict = RestCalculator.make_feature_dict(raw)

        if self._check_force_rest(feature_dict["work_duration_min"]):
            return {
                "worker_id": feature_dict["worker_id"],
                "result": FINAL_FORCE_REST,
                "reason": (
                    f"작업시간 {feature_dict['work_duration_min']}분이 "
                    f"임계치 {self.forced_rest_work_min}분 이상"
                ),
                "heat_index": feature_dict["heat_index"],
                "baseline_hr": feature_dict["baseline_hr"],
                "hr_delta_from_baseline": feature_dict["hr_delta_from_baseline"],
                "probabilities": None,
            }

        x = pd.DataFrame(
            [[feature_dict[col] for col in self.feature_cols]],
            columns=self.feature_cols,
        )

        pred_label = int(self.model.predict(x)[0])
        pred_proba = self.model.predict_proba(x)[0]
        classes = self.model.named_steps["clf"].classes_

        proba_map = {
            self.label_name_map[int(label)]: round(float(prob), 4)
            for label, prob in zip(classes, pred_proba)
        }

        return {
            "worker_id": feature_dict["worker_id"],
            "result": self.label_name_map[pred_label],
            "reason": "모델 예측 결과",
            "heat_index": feature_dict["heat_index"],
            "baseline_hr": feature_dict["baseline_hr"],
            "hr_delta_from_baseline": feature_dict["hr_delta_from_baseline"],
            "probabilities": proba_map,
        }
