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
        print(
            "[RestModelEngine] START init "
            f"model_path={model_path}, forced_rest_work_min={forced_rest_work_min}"
        )
        payload = joblib.load(model_path)
        self.model = payload["model"]
        self.feature_cols: List[str] = payload["feature_cols"]
        self.label_name_map: Dict[int, str] = payload["label_name_map"]
        self.forced_rest_work_min = forced_rest_work_min
        self.worker_baseline_map: Dict[str, float] = {}
        print(
            "[RestModelEngine] END init "
            f"feature_cols={self.feature_cols}, label_name_map={self.label_name_map}"
        )

    def reset_worker(self, worker_id: str) -> None:
        print(f"[RestModelEngine] START reset_worker worker_id={worker_id}")
        if worker_id in self.worker_baseline_map:
            del self.worker_baseline_map[worker_id]
            print(f"[RestModelEngine] reset_worker removed baseline for worker_id={worker_id}")
        else:
            print(f"[RestModelEngine] reset_worker no baseline found for worker_id={worker_id}")
        print(f"[RestModelEngine] END reset_worker worker_id={worker_id}")

    def _inject_baseline(self, raw: WorkerRawInput) -> WorkerRawInput:
        print(
            "[RestModelEngine] START _inject_baseline "
            f"worker_id={raw.worker_id}, current_hr={raw.hr}, incoming_baseline={raw.baseline_hr}"
        )
        if raw.baseline_hr is not None:
            self.worker_baseline_map[raw.worker_id] = float(raw.baseline_hr)
            print(
                "[RestModelEngine] END _inject_baseline "
                f"source=incoming, baseline={raw.baseline_hr}"
            )
            return raw

        if raw.worker_id not in self.worker_baseline_map:
            self.worker_baseline_map[raw.worker_id] = float(raw.hr)
            print(
                "[RestModelEngine] _inject_baseline new baseline from current_hr "
                f"baseline={self.worker_baseline_map[raw.worker_id]}"
            )

        raw.baseline_hr = self.worker_baseline_map[raw.worker_id]
        print(
            "[RestModelEngine] END _inject_baseline "
            f"source=cache, baseline={raw.baseline_hr}"
        )
        return raw

    def _check_force_rest(self, work_duration_min: int) -> bool:
        forced = int(work_duration_min) >= int(self.forced_rest_work_min)
        print(
            "[RestModelEngine] _check_force_rest "
            f"work_duration_min={work_duration_min}, "
            f"threshold={self.forced_rest_work_min}, forced={forced}"
        )
        return forced

    def predict(self, raw: WorkerRawInput) -> Dict:
        print(f"[RestModelEngine] START predict raw={raw}")
        raw = self._inject_baseline(raw)
        feature_dict = RestCalculator.make_feature_dict(raw)
        print(f"[RestModelEngine] feature_dict={feature_dict}")

        if self._check_force_rest(feature_dict["work_duration_min"]):
            result = {
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
            print(f"[RestModelEngine] END predict forced result={result}")
            return result

        print(
            "[RestModelEngine] model input START "
            f"feature_cols={self.feature_cols}"
        )
        x = pd.DataFrame(
            [[feature_dict[col] for col in self.feature_cols]],
            columns=self.feature_cols,
        )
        print(f"[RestModelEngine] model input dataframe={x.to_dict(orient='records')}")

        pred_label = int(self.model.predict(x)[0])
        pred_proba = self.model.predict_proba(x)[0]
        classes = self.model.named_steps["clf"].classes_
        print(
            "[RestModelEngine] model output "
            f"pred_label={pred_label}, classes={list(classes)}, pred_proba={pred_proba.tolist()}"
        )

        proba_map = {
            self.label_name_map[int(label)]: round(float(prob), 4)
            for label, prob in zip(classes, pred_proba)
        }

        result = {
            "worker_id": feature_dict["worker_id"],
            "result": self.label_name_map[pred_label],
            "reason": "모델 예측 결과",
            "heat_index": feature_dict["heat_index"],
            "baseline_hr": feature_dict["baseline_hr"],
            "hr_delta_from_baseline": feature_dict["hr_delta_from_baseline"],
            "probabilities": proba_map,
        }
        print(f"[RestModelEngine] END predict result={result}")
        return result
