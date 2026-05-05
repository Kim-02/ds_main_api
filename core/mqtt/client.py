import asyncio
import json
import logging

import aiomqtt

from config import settings
from core.mqtt.topics import Topics

logger = logging.getLogger(__name__)

_client: aiomqtt.Client | None = None
_task: asyncio.Task | None = None


async def publish(topic: str, payload: dict) -> None:
    if _client is None:
        logger.warning("MQTT client not connected, skipping publish to %s", topic)
        return
    await _client.publish(topic, json.dumps(payload))


async def _run_loop(client: aiomqtt.Client) -> None:
    from temperature.mqtt.handler import handle as temp_handle
    from heartrate.mqtt.handler import handle as hr_handle

    await client.subscribe(Topics.TEMPERATURE_WILDCARD)
    await client.subscribe(Topics.HEARTRATE_WILDCARD)
    await client.subscribe(Topics.BAND_REGISTER)

    logger.info("MQTT subscriptions active")

    async for message in client.messages:
        topic = str(message.topic)
        try:
            payload = json.loads(message.payload)
        except (json.JSONDecodeError, ValueError):
            logger.warning("Non-JSON MQTT message on %s", topic)
            continue

        try:
            if topic.startswith("sensors/temperature/"):
                device_id = topic.split("/")[-1]
                asyncio.create_task(temp_handle(device_id, payload))
            elif topic.startswith("sensors/heartrate/"):
                device_id = topic.split("/")[-1]
                asyncio.create_task(hr_handle(device_id, payload))
        except Exception:
            logger.exception("Error dispatching MQTT message on %s", topic)


async def start() -> None:
    global _client, _task

    kwargs: dict = {
        "hostname": settings.mqtt_broker_host,
        "port": settings.mqtt_broker_port,
    }
    if settings.mqtt_username:
        kwargs["username"] = settings.mqtt_username
        kwargs["password"] = settings.mqtt_password

    async def _connect_loop() -> None:
        global _client
        while True:
            try:
                async with aiomqtt.Client(**kwargs) as client:
                    _client = client
                    logger.info("MQTT connected to %s:%s", settings.mqtt_broker_host, settings.mqtt_broker_port)
                    await _run_loop(client)
            except aiomqtt.MqttError as e:
                logger.error("MQTT error: %s — reconnecting in 5s", e)
                _client = None
                await asyncio.sleep(5)

    _task = asyncio.create_task(_connect_loop())


async def stop() -> None:
    global _task
    if _task:
        _task.cancel()
        _task = None
