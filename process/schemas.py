from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ProcessCreate(BaseModel):
    name: str
    description: Optional[str] = None
    location: Optional[str] = None
    precautions: Optional[str] = None


class ProcessUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    location: Optional[str] = None
    precautions: Optional[str] = None
    is_active: Optional[bool] = None


class ProcessOut(BaseModel):
    id: int
    name: str
    description: Optional[str]
    location: Optional[str]
    precautions: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
