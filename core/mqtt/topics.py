class Topics:
    SENSOR_STATUS_WILDCARD = "sensors/+/status"
    SENSOR_TELEMETRY_WILDCARD = "sensors/+/telemetry"

    @staticmethod
    def sensor_base(sensor_id: str) -> str:
        return f"sensors/{sensor_id}"

    @staticmethod
    def sensor_status(sensor_id: str) -> str:
        return f"{Topics.sensor_base(sensor_id)}/status"

    @staticmethod
    def sensor_telemetry(sensor_id: str) -> str:
        return f"{Topics.sensor_base(sensor_id)}/telemetry"

    @staticmethod
    def sensor_cmd(sensor_id: str) -> str:
        return f"{Topics.sensor_base(sensor_id)}/cmd"

    @staticmethod
    def sensor_alert(sensor_id: str) -> str:
        return f"{Topics.sensor_base(sensor_id)}/alert"
