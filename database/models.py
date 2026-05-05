import enum
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean, DateTime, Enum, Float, ForeignKey,
    Integer, JSON, String, Text, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class SensorType(str, enum.Enum):
    temperature = "temperature"
    heartrate_band = "heartrate_band"
    camera = "camera"


class AnomalyType(str, enum.Enum):
    temperature = "temperature"
    heartrate = "heartrate"
    cctv = "cctv"


class AnomalyStatus(str, enum.Enum):
    active = "active"
    resolved = "resolved"
    stopped = "stopped"


# ---------------------------------------------------------------------------
# Process (공정)
# ---------------------------------------------------------------------------

class Process(Base):
    __tablename__ = "processes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    location: Mapped[Optional[str]] = mapped_column(String(255))
    precautions: Mapped[Optional[str]] = mapped_column(Text)  # 해당 공정의 주의 사항
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    workers: Mapped[list["Worker"]] = relationship("Worker", back_populates="process")
    sensors: Mapped[list["Sensor"]] = relationship("Sensor", back_populates="process")
    anomaly_events: Mapped[list["AnomalyEvent"]] = relationship("AnomalyEvent", back_populates="process")


# ---------------------------------------------------------------------------
# Worker (작업자)
# ---------------------------------------------------------------------------

class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    employee_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    process_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("processes.id"), nullable=True)
    # 개인 건강 기저 데이터 (resting heart rate, age, etc.)
    health_baseline: Mapped[Optional[dict]] = mapped_column(JSON)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    process: Mapped[Optional["Process"]] = relationship("Process", back_populates="workers")
    heartrate_band: Mapped[Optional["HeartRateBand"]] = relationship(
        "HeartRateBand", back_populates="worker", uselist=False
    )


# ---------------------------------------------------------------------------
# Sensor base + subtypes
# ---------------------------------------------------------------------------

class Sensor(Base):
    __tablename__ = "sensors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sensor_type: Mapped[SensorType] = mapped_column(Enum(SensorType), nullable=False)
    device_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    process_id: Mapped[int] = mapped_column(Integer, ForeignKey("processes.id"), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    process: Mapped["Process"] = relationship("Process", back_populates="sensors")
    readings: Mapped[list["SensorReading"]] = relationship("SensorReading", back_populates="sensor")
    anomaly_events: Mapped[list["AnomalyEvent"]] = relationship("AnomalyEvent", back_populates="sensor")

    temperature_sensor: Mapped[Optional["TemperatureSensor"]] = relationship(
        "TemperatureSensor", back_populates="sensor", uselist=False
    )
    heartrate_band: Mapped[Optional["HeartRateBand"]] = relationship(
        "HeartRateBand", back_populates="sensor", uselist=False
    )
    camera: Mapped[Optional["Camera"]] = relationship(
        "Camera", back_populates="sensor", uselist=False
    )


class TemperatureSensor(Base):
    __tablename__ = "temperature_sensors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sensor_id: Mapped[int] = mapped_column(Integer, ForeignKey("sensors.id"), unique=True)
    threshold_temperature: Mapped[float] = mapped_column(Float, default=35.0)
    threshold_humidity: Mapped[float] = mapped_column(Float, default=80.0)

    sensor: Mapped["Sensor"] = relationship("Sensor", back_populates="temperature_sensor")


class HeartRateBand(Base):
    __tablename__ = "heartrate_bands"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sensor_id: Mapped[int] = mapped_column(Integer, ForeignKey("sensors.id"), unique=True)
    worker_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("workers.id"), nullable=True)
    threshold_heartrate: Mapped[int] = mapped_column(Integer, default=120)

    sensor: Mapped["Sensor"] = relationship("Sensor", back_populates="heartrate_band")
    worker: Mapped[Optional["Worker"]] = relationship("Worker", back_populates="heartrate_band")


class Camera(Base):
    __tablename__ = "cameras"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sensor_id: Mapped[int] = mapped_column(Integer, ForeignKey("sensors.id"), unique=True)
    rtsp_url: Mapped[str] = mapped_column(String(512), nullable=False)

    sensor: Mapped["Sensor"] = relationship("Sensor", back_populates="camera")
    frame_records: Mapped[list["FrameRecord"]] = relationship("FrameRecord", back_populates="camera")


# ---------------------------------------------------------------------------
# SensorReading (원시 센서 데이터)
# ---------------------------------------------------------------------------

class SensorReading(Base):
    __tablename__ = "sensor_readings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sensor_id: Mapped[int] = mapped_column(Integer, ForeignKey("sensors.id"), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)
    data: Mapped[dict] = mapped_column(JSON, nullable=False)  # {"temperature": 36.5, "humidity": 60.0}

    sensor: Mapped["Sensor"] = relationship("Sensor", back_populates="readings")


# ---------------------------------------------------------------------------
# FrameRecord (CCTV 프레임 + YOLO 분석)
# ---------------------------------------------------------------------------

class FrameRecord(Base):
    __tablename__ = "frame_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    camera_id: Mapped[int] = mapped_column(Integer, ForeignKey("cameras.id"), nullable=False, index=True)
    process_id: Mapped[int] = mapped_column(Integer, ForeignKey("processes.id"), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)
    frame_path: Mapped[str] = mapped_column(String(512), nullable=False)
    yolo_detections: Mapped[Optional[dict]] = mapped_column(JSON)  # YOLO 탐지 결과
    is_anomaly: Mapped[bool] = mapped_column(Boolean, default=False)

    camera: Mapped["Camera"] = relationship("Camera", back_populates="frame_records")


# ---------------------------------------------------------------------------
# AnomalyEvent (이상 이벤트)
# ---------------------------------------------------------------------------

class AnomalyEvent(Base):
    __tablename__ = "anomaly_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    anomaly_type: Mapped[AnomalyType] = mapped_column(Enum(AnomalyType), nullable=False)
    process_id: Mapped[int] = mapped_column(Integer, ForeignKey("processes.id"), nullable=False, index=True)
    sensor_id: Mapped[int] = mapped_column(Integer, ForeignKey("sensors.id"), nullable=False)
    triggered_value: Mapped[Optional[float]] = mapped_column(Float)  # 트리거된 값 (온도, 심박수 등)
    start_time: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[AnomalyStatus] = mapped_column(Enum(AnomalyStatus), default=AnomalyStatus.active)

    process: Mapped["Process"] = relationship("Process", back_populates="anomaly_events")
    sensor: Mapped["Sensor"] = relationship("Sensor", back_populates="anomaly_events")
    vlm_queries: Mapped[list["VLMQuery"]] = relationship("VLMQuery", back_populates="anomaly_event")
    report: Mapped[Optional["Report"]] = relationship("Report", back_populates="anomaly_event", uselist=False)


# ---------------------------------------------------------------------------
# VLMQuery (VLM 호출 이력)
# ---------------------------------------------------------------------------

class VLMQuery(Base):
    __tablename__ = "vlm_queries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    anomaly_event_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("anomaly_events.id"), nullable=False, index=True
    )
    queried_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    response: Mapped[Optional[str]] = mapped_column(Text)
    frame_path: Mapped[Optional[str]] = mapped_column(String(512))  # 전달된 프레임 경로

    anomaly_event: Mapped["AnomalyEvent"] = relationship("AnomalyEvent", back_populates="vlm_queries")


# ---------------------------------------------------------------------------
# Report (상황 조치 리포트)
# ---------------------------------------------------------------------------

class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    anomaly_event_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("anomaly_events.id"), unique=True, nullable=False
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    action_taken: Mapped[Optional[str]] = mapped_column(Text)
    created_by: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    anomaly_event: Mapped["AnomalyEvent"] = relationship("AnomalyEvent", back_populates="report")
