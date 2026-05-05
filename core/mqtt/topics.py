class Topics:
    TEMPERATURE_WILDCARD = "sensors/temperature/+"
    HEARTRATE_WILDCARD = "sensors/heartrate/+"
    BAND_REGISTER = "band/register"

    @staticmethod
    def temperature(device_id: str) -> str:
        return f"sensors/temperature/{device_id}"

    @staticmethod
    def heartrate(device_id: str) -> str:
        return f"sensors/heartrate/{device_id}"

    @staticmethod
    def band_alert(device_id: str) -> str:
        return f"band/alert/{device_id}"
