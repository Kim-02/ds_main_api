from ai.rest import DatabaseHandlerRestDataRepository, RestRuntimeService
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from database.db_handler import DatabaseHandler


def build_rest_service(db_handler: "DatabaseHandler") -> RestRuntimeService:
    repository = DatabaseHandlerRestDataRepository(db_handler)
    return RestRuntimeService.from_model_path(repository=repository)


def evaluate_and_build_command(db_handler: DatabaseHandler, worker_id: str):
    service = build_rest_service(db_handler)
    result = service.evaluate_worker(worker_id)

    if not result.should_rest:
        return result, None, None

    topic, payload = result.command.to_topic_and_payload()
    return result, topic, payload


if __name__ == "__main__":
    from config import settings
    from database.db_handler import DatabaseHandler

    db = DatabaseHandler(
        host=settings.mariadb_host,
        user=settings.mariadb_user,
        password=settings.mariadb_password,
        db_name=settings.mariadb_db_name,
        port=settings.mariadb_port,
    )
    prediction, target_topic, message = evaluate_and_build_command(db, "1")
    print(prediction.prediction)
    if target_topic and message:
        print(target_topic)
        print(message)
