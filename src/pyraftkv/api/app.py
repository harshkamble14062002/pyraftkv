from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from pyraftkv.storage.store import KVStore

app = FastAPI(
    title="PyRaftKV",
    description="Distributed key-value store built from scratch in Python",
    version="0.1.0",
)

store = KVStore()


class PutRequest(BaseModel):
    value: str


@app.put("/kv/{key}")
def put_value(key: str, request: PutRequest) -> dict[str, str]:
    store.put(key, request.value)

    return {
        "key": key,
        "value": request.value,
    }


@app.get("/kv/{key}")
def get_value(key: str) -> dict[str, str]:
    value = store.get(key)

    if value is None:
        raise HTTPException(
            status_code=404,
            detail="Key not found",
        )

    return {
        "key": key,
        "value": value,
    }


@app.delete("/kv/{key}")
def delete_value(key: str) -> dict[str, str]:
    deleted = store.delete(key)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail="Key not found",
        )

    return {
        "key": key,
        "status": "deleted",
    }

