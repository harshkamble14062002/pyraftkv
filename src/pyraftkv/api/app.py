from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from pyraftkv.storage.engine import StorageEngine


class PutRequest(BaseModel):
    value: str


def create_app(
    wal_path: str | Path = "data/pyraftkv.wal",
) -> FastAPI:
    app = FastAPI(
        title="PyRaftKV",
        description="Distributed key-value store built from scratch in Python",
        version="0.1.0",
    )

    engine = StorageEngine(wal_path)

    @app.put("/kv/{key}")
    def put_value(key: str, request: PutRequest) -> dict[str, str]:
        engine.put(key, request.value)

        return {
            "key": key,
            "value": request.value,
        }

    @app.get("/kv/{key}")
    def get_value(key: str) -> dict[str, str]:
        value = engine.get(key)

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
        deleted = engine.delete(key)

        if not deleted:
            raise HTTPException(
                status_code=404,
                detail="Key not found",
            )

        return {
            "key": key,
            "status": "deleted",
        }

    return app


app = create_app()

