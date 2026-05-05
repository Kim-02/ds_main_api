import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_app = None


def _get_app():
    global _app
    if _app is not None:
        return _app

    from config import settings
    cred_path = Path(settings.firebase_credentials_path)
    if not cred_path.exists():
        logger.warning("Firebase credentials not found at %s — push disabled", cred_path)
        return None

    import firebase_admin
    from firebase_admin import credentials
    cred = credentials.Certificate(str(cred_path))
    _app = firebase_admin.initialize_app(cred)
    return _app


async def send_anomaly_alert(
    process_id: int,
    anomaly_type: str,
    message: str,
    event_id: int,
) -> None:
    app = _get_app()
    if app is None:
        logger.debug("Push skipped (no Firebase credentials)")
        return

    from firebase_admin import messaging

    title_map = {
        "temperature": "온도 이상 감지",
        "heartrate": "심박 이상 감지",
        "cctv": "CCTV 위험 감지",
    }

    msg = messaging.Message(
        notification=messaging.Notification(
            title=title_map.get(anomaly_type, "이상 감지"),
            body=message[:200],
        ),
        data={
            "event_id": str(event_id),
            "process_id": str(process_id),
            "anomaly_type": anomaly_type,
        },
        topic=f"process_{process_id}",
    )

    try:
        response = messaging.send(msg)
        logger.info("Push sent for event %s: %s", event_id, response)
    except Exception:
        logger.exception("Failed to send push for event %s", event_id)
