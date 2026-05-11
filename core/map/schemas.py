from pydantic import BaseModel


class SensorPositionSaveReq(BaseModel):
    map_id: int
    sensor_id: str
    x_ratio: float
    y_ratio: float

