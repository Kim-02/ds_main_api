import json
import unittest

from ai.rest import (
    EnvironmentSample,
    FINAL_NO_REST,
    FINAL_STRONG_REST,
    RestRuntimeService,
    WatchSample,
    WorkerProfile,
)
from ai.rest.mariadb_repository import DatabaseHandlerRestDataRepository


class FakeRepository:
    def __init__(self, target_topic: str = "sensors/band-01/alert"):
        self.target_topic = target_topic

    def fetch_environment(self, worker_id: str) -> EnvironmentSample:
        return EnvironmentSample(temp_c=33.5, humid=68.0)

    def fetch_watch(self, worker_id: str) -> WatchSample:
        return WatchSample(hr=128.0, baseline_hr=88.0)

    def fetch_worker_profile(self, worker_id: str) -> WorkerProfile:
        return WorkerProfile(
            worker_id=worker_id,
            age=61,
            gender=1,
            height_cm=170.0,
            weight_kg=68.0,
            work_duration_min=95,
            elderly_flag=1,
            heart_disease=0,
            hypertension=1,
            other_disease=0,
            target_topic=self.target_topic,
        )


class FakeEngine:
    def __init__(self, result: str):
        self.result = result
        self.last_raw = None

    def predict(self, raw):
        self.last_raw = raw
        return {
            "worker_id": raw.worker_id,
            "result": self.result,
            "reason": "fake",
            "heat_index": 34.68,
            "baseline_hr": raw.baseline_hr,
            "hr_delta_from_baseline": raw.hr - raw.baseline_hr,
            "probabilities": None,
        }


class FakeWatchPublisher:
    def __init__(self):
        self.published = []

    def publish(self, topic: str, payload: str):
        self.published.append((topic, payload))


def run_dummy_rest_alert_pipeline(
    *,
    worker_id: str = "worker_01",
    repository=None,
    publisher=None,
):
    repository = repository or FakeRepository()
    publisher = publisher or FakeWatchPublisher()
    service = RestRuntimeService.from_model_path(repository=repository)

    result = service.evaluate_worker(worker_id)
    if result.should_rest and result.command is not None:
        topic, payload = result.command.to_topic_and_payload()
        publisher.publish(topic, payload)
        return {
            "result": result,
            "topic": topic,
            "payload": payload,
            "publisher": publisher,
        }

    return {
        "result": result,
        "topic": None,
        "payload": None,
        "publisher": publisher,
    }


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.last_query = ""
        self.last_params = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=()):
        self.last_query = " ".join(query.lower().split())
        self.last_params = params

    def fetchone(self):
        query = self.last_query
        if "from th_trans" in query:
            return self.rows["environment"]
        if "from hb_trans" in query:
            return self.rows["watch"]
        if "join sensor s" in query and "where s.sensor_id" in query:
            return self.rows["worker_by_sensor"]
        if "from worker w" in query:
            return self.rows["worker_profile"]
        return None


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return FakeCursor(self.rows)


class FakeDbHandler:
    def __init__(self):
        self.rows = {
            "environment": {"temp_c": 31.2, "humid": 62.5},
            "watch": {"hr": 118.0},
            "worker_profile": {
                "dept_id": 1001,
                "age": 59,
                "gender": "male",
                "height_cm": 172.0,
                "weight_kg": 73.0,
                "work_duration_min": 45,
                "elderly_flag": 0,
                "heart_disease": 0,
                "hypertension": 1,
                "other_disease": 0,
                "sensor_id": "band-01",
                "mqtt_topic": "sensors/band-01/telemetry",
            },
            "worker_by_sensor": {"dept_id": 1001},
        }

    def _get_connection(self):
        return FakeConnection(self.rows)


class RestRuntimeServiceTest(unittest.TestCase):
    def test_rest_prediction_builds_yellow_band_command(self):
        engine = FakeEngine(FINAL_STRONG_REST)
        service = RestRuntimeService(
            repository=FakeRepository(),
            engine=engine,
        )

        result = service.evaluate_worker("worker_01")

        self.assertTrue(result.should_rest)
        self.assertEqual(engine.last_raw.hr, 128.0)
        self.assertEqual(engine.last_raw.temp_c, 33.5)
        self.assertEqual(engine.last_raw.humid, 68.0)

        command = result.command.to_dict()
        self.assertEqual(command["target_topic"], "sensors/band-01/alert")
        self.assertEqual(command["command"], "alert_on")
        self.assertEqual(command["color"], "yellow")
        self.assertTrue(command["vibration"])
        self.assertTrue(command["led"])

    def test_no_rest_prediction_does_not_build_command(self):
        service = RestRuntimeService(
            repository=FakeRepository(),
            engine=FakeEngine(FINAL_NO_REST),
        )

        result = service.evaluate_worker("worker_01")

        self.assertFalse(result.should_rest)
        self.assertIsNone(result.command_json())

    def test_topic_and_payload_for_mqtt_publish(self):
        service = RestRuntimeService(
            repository=FakeRepository(target_topic="sensors/band-99/alert"),
            engine=FakeEngine(FINAL_STRONG_REST),
        )

        result = service.evaluate_worker("worker_99")
        topic, payload = result.command.to_topic_and_payload()

        self.assertEqual(topic, "sensors/band-99/alert")
        self.assertNotIn("target_topic", json.loads(payload))
        self.assertEqual(json.loads(payload)["command"], "alert_on")
        self.assertEqual(json.loads(payload)["color"], "yellow")


class RestAlertPipelineTest(unittest.TestCase):
    def test_dummy_data_model_builds_json_and_sends_watch_alert(self):
        publisher = FakeWatchPublisher()

        pipeline_result = run_dummy_rest_alert_pipeline(
            worker_id="worker_01",
            repository=FakeRepository(target_topic="sensors/band-01/alert"),
            publisher=publisher,
        )

        result = pipeline_result["result"]
        payload = pipeline_result["payload"]

        self.assertTrue(result.should_rest)
        self.assertEqual(result.prediction["result"], "강한휴식권고")
        self.assertEqual(pipeline_result["topic"], "sensors/band-01/alert")
        self.assertEqual(len(publisher.published), 1)
        self.assertEqual(publisher.published[0], ("sensors/band-01/alert", payload))

        watch_json = json.loads(payload)
        self.assertEqual(watch_json["command"], "alert_on")
        self.assertEqual(watch_json["color"], "yellow")
        self.assertTrue(watch_json["vibration"])
        self.assertTrue(watch_json["led"])
        self.assertEqual(watch_json["duration_ms"], 5000)
        self.assertEqual(watch_json["reset_after_ms"], 15000)
        self.assertNotIn("target_topic", watch_json)


class DatabaseHandlerRestDataRepositoryTest(unittest.TestCase):
    def test_repository_reads_existing_main_db_shape(self):
        repository = DatabaseHandlerRestDataRepository(FakeDbHandler())

        environment = repository.fetch_environment("1001")
        watch = repository.fetch_watch("1001")
        profile = repository.fetch_worker_profile("1001")

        self.assertEqual(environment.temp_c, 31.2)
        self.assertEqual(environment.humid, 62.5)
        self.assertEqual(watch.hr, 118.0)
        self.assertEqual(profile.worker_id, "1001")
        self.assertEqual(profile.gender, 1)
        self.assertEqual(profile.target_topic, "sensors/band-01/alert")
        self.assertEqual(repository.find_worker_id_by_sensor_id("band-01"), "1001")


if __name__ == "__main__":
    unittest.main()
