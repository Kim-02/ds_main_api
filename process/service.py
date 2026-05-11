from datetime import datetime

from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool

from .schemas import ProcessCreate, ProcessUpdate


def _row_to_out(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": row["space_id"],
        "name": row["space_name"],
        "description": None,
        "location": None,
        "precautions": None,
        "is_active": True,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }


async def list_processes(db) -> list[dict]:
    rows = await run_in_threadpool(db.get_all_spaces)
    return [_row_to_out(r) for r in rows]


async def get_process(db, process_id: int) -> dict:
    row = await run_in_threadpool(db.get_space_by_id, process_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Process not found")
    return _row_to_out(row)


async def create_process(db, data: ProcessCreate) -> dict:
    # ds_space는 space_name만 저장. description/location/precautions/is_active는 무시됨.
    row = await run_in_threadpool(db.create_space, data.name)
    if not row:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="생성 실패")
    return _row_to_out(row)


async def update_process(db, process_id: int, data: ProcessUpdate) -> dict:
    await get_process(db, process_id)
    name = data.name
    if name is None:
        row = await run_in_threadpool(db.get_space_by_id, process_id)
        name = row["space_name"]
    row = await run_in_threadpool(db.update_space, process_id, name)
    return _row_to_out(row)


async def delete_process(db, process_id: int) -> None:
    await get_process(db, process_id)
    await run_in_threadpool(db.delete_space, process_id)
