from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class WorkerCreate(BaseModel):
    employee_id: str
    name: str
    process_id: Optional[int] = None
    health_baseline: Optional[dict[str, Any]] = None


class WorkerUpdate(BaseModel):
    name: Optional[str] = None
    process_id: Optional[int] = None
    health_baseline: Optional[dict[str, Any]] = None
    is_active: Optional[bool] = None


class WorkerOut(BaseModel):
    id: int
    employee_id: str
    name: str
    process_id: Optional[int]
    health_baseline: Optional[dict[str, Any]]
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}
