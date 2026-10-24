
"""IoMT Data Mesh ingestion and telemetry read API."""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field, ValidationError


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("iomt-data-mesh")

REGISTRY_PATH = Path(
    os.getenv(
        "IOMT_REGISTRY_PATH",
        Path(__file__).resolve().parents[1] / "device_registry.json",
    )
)

DATABASE_URL = os.getenv(
    "IOMT_DATABASE_URL",
    "postgresql://monika@localhost:5432/iomt_db",
)


class TelemetryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_mesh_id: str = Field(min_length=1)
    registered_device_id: str = Field(min_length=1)
    device_type: str = Field(min_length=1)
    timestamp: datetime
    telemetry: dict[str, Any]


def load_registry() -> dict[str, dict[str, str]]:
    """Load configured device IDs, mesh IDs, and device types."""
    try:
        payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        devices = payload["devices"]
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise RuntimeError(
            f"Cannot load device registry at {REGISTRY_PATH}: {exc}"
        ) from exc

    if not isinstance(devices, list) or not devices:
        raise RuntimeError(
            "Device registry must contain a non-empty 'devices' list"
        )

    registry: dict[str, dict[str, str]] = {}

    for device in devices:
        if not isinstance(device, dict):
            raise RuntimeError("Each registry device must be a JSON object")

        device_id = device.get("registered_device_id")
        mesh_id = device.get("data_mesh_id")
        device_type = device.get("device_type")

        if not all(
            isinstance(value, str) and value.strip()
            for value in (device_id, mesh_id, device_type)
        ):
            raise RuntimeError(f"Invalid device registry entry: {device}")

        if device_id in registry:
            raise RuntimeError(f"Duplicate registered_device_id: {device_id}")

        registry[device_id] = {
            "data_mesh_id": mesh_id,
            "device_type": device_type,
        }

    return registry


DEVICE_REGISTRY = load_registry()


def initialize_database() -> None:
    """Create the telemetry table and index if necessary."""
    with psycopg.connect(DATABASE_URL, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry (
                    id BIGSERIAL PRIMARY KEY,
                    data_mesh_id VARCHAR(50) NOT NULL,
                    registered_device_id VARCHAR(50) NOT NULL,
                    device_type VARCHAR(100) NOT NULL,
                    device_timestamp TIMESTAMPTZ NOT NULL,
                    telemetry JSONB NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL
                        DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
                ON telemetry (registered_device_id, received_at DESC)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_telemetry_mesh_time
                ON telemetry (data_mesh_id, received_at DESC)
                """
            )

    logger.info("PostgreSQL telemetry table is ready")


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    yield


app = FastAPI(
    title="IoMT Data Mesh API",
    version="1.3.0",
    lifespan=lifespan,
)


def save_telemetry(message: TelemetryMessage) -> int:
    """Persist one validated telemetry reading."""
    with psycopg.connect(DATABASE_URL, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO telemetry (
                    data_mesh_id,
                    registered_device_id,
                    device_type,
                    device_timestamp,
                    telemetry
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                RETURNING id
                """,
                (
                    message.data_mesh_id,
                    message.registered_device_id,
                    message.device_type,
                    message.timestamp,
                    json.dumps(message.telemetry),
                ),
            )

            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("Database did not return the telemetry ID")

            return row[0]


def serialize_record(row: tuple) -> dict[str, Any]:
    """Convert a database row into JSON-compatible values."""
    return {
        "id": row[0],
        "data_mesh_id": row[1],
        "registered_device_id": row[2],
        "device_type": row[3],
        "timestamp": row[4].isoformat(),
        "telemetry": row[5],
        "received_at": row[6].isoformat(),
    }


@app.get("/")
def home():
    return {
        "message": "IoMT Data Mesh is running",
        "websocket": "/ws",
        "health": "/health",
        "telemetry_api": "/api/telemetry",
        "devices_api": "/api/devices",
        "configured_devices": len(DEVICE_REGISTRY),
        "data_mesh_ids": sorted(
            {device["data_mesh_id"] for device in DEVICE_REGISTRY.values()}
        ),
    }


@app.get("/health")
def health():
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=3) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

        return {
            "status": "ok",
            "database": "connected",
            "configured_devices": len(DEVICE_REGISTRY),
        }

    except psycopg.Error:
        logger.exception("Database health check failed")
        raise HTTPException(
            status_code=503,
            detail={
                "status": "degraded",
                "database": "disconnected",
                "configured_devices": len(DEVICE_REGISTRY),
            },
        )


@app.get("/api/telemetry")
def get_telemetry(
    data_mesh_id: str = Query(min_length=1, max_length=50),
    registered_device_id: str | None = Query(default=None, max_length=150),
    limit: int = Query(default=100, ge=1, le=500),
):
    """Retrieve recent persisted telemetry for a mesh or one of its devices."""

    mesh_id = data_mesh_id.strip()
    device_id = (
        registered_device_id.strip()
        if registered_device_id is not None
        else None
    )

    if not mesh_id:
        raise HTTPException(status_code=400, detail="data_mesh_id is required")

    # Reject unknown devices and devices belonging to another mesh.
    if device_id:
        registered = DEVICE_REGISTRY.get(device_id)
        if registered is None or registered["data_mesh_id"] != mesh_id:
            raise HTTPException(status_code=404, detail="Device not found")

    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                if device_id:
                    cursor.execute(
                        """
                        SELECT id, data_mesh_id, registered_device_id,
                               device_type, device_timestamp, telemetry,
                               received_at
                        FROM telemetry
                        WHERE data_mesh_id = %s
                          AND registered_device_id = %s
                        ORDER BY received_at DESC, id DESC
                        LIMIT %s
                        """,
                        (mesh_id, device_id, limit),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT id, data_mesh_id, registered_device_id,
                               device_type, device_timestamp, telemetry,
                               received_at
                        FROM telemetry
                        WHERE data_mesh_id = %s
                        ORDER BY received_at DESC, id DESC
                        LIMIT %s
                        """,
                        (mesh_id, limit),
                    )

                records = [
                    serialize_record(row) for row in cursor.fetchall()
                ]

        return {
            "data_mesh_id": mesh_id,
            "count": len(records),
            "records": records,
        }

    except psycopg.Error as exc:
        logger.exception("Failed to read telemetry")
        raise HTTPException(
            status_code=503,
            detail="Telemetry database is temporarily unavailable",
        ) from exc


@app.get("/api/devices")
def get_devices(
    data_mesh_id: str = Query(min_length=1, max_length=50),
):
    """List registry devices and the latest persisted reading for each."""

    mesh_id = data_mesh_id.strip()
    if not mesh_id:
        raise HTTPException(status_code=400, detail="data_mesh_id is required")

    configured = [
        {"registered_device_id": device_id, **details}
        for device_id, details in DEVICE_REGISTRY.items()
        if details["data_mesh_id"] == mesh_id
    ]

    if not configured:
        raise HTTPException(status_code=404, detail="Data mesh not found")

    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON (registered_device_id)
                           id, data_mesh_id, registered_device_id,
                           device_type, device_timestamp, telemetry,
                           received_at
                    FROM telemetry
                    WHERE data_mesh_id = %s
                    ORDER BY registered_device_id, received_at DESC, id DESC
                    """,
                    (mesh_id,),
                )
                rows = cursor.fetchall()

        latest_by_device = {
            row[2]: serialize_record(row) for row in rows
        }

        return {
            "data_mesh_id": mesh_id,
            "count": len(configured),
            "devices": [
                {
                    **device,
                    "latest_reading": latest_by_device.get(
                        device["registered_device_id"]
                    ),
                }
                for device in configured
            ],
        }

    except psycopg.Error as exc:
        logger.exception("Failed to read device telemetry")
        raise HTTPException(
            status_code=503,
            detail="Telemetry database is temporarily unavailable",
        ) from exc


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Receive, validate, and persist simulator/device telemetry."""
    await websocket.accept()

    client = websocket.client.host if websocket.client else "unknown"
    logger.info("WebSocket client connected: %s", client)

    try:
        while True:
            raw_data = await websocket.receive_json()

            try:
                message = TelemetryMessage.model_validate(raw_data)
            except (ValidationError, TypeError, ValueError) as exc:
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "invalid_message",
                        "detail": str(exc),
                    }
                )
                continue

            registered = DEVICE_REGISTRY.get(message.registered_device_id)

            if registered is None:
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "unknown_registered_device",
                        "registered_device_id": message.registered_device_id,
                    }
                )
                continue

            if registered["data_mesh_id"] != message.data_mesh_id:
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "data_mesh_mismatch",
                        "registered_device_id": message.registered_device_id,
                    }
                )
                continue

            if registered["device_type"] != message.device_type:
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "device_type_mismatch",
                        "registered_device_id": message.registered_device_id,
                    }
                )
                continue

            try:
                record_id = save_telemetry(message)
            except (psycopg.Error, RuntimeError):
                logger.exception(
                    "Failed to persist telemetry for device %s",
                    message.registered_device_id,
                )
                await websocket.send_json(
                    {
                        "status": "error",
                        "reason": "database_write_failed",
                        "registered_device_id": message.registered_device_id,
                    }
                )
                continue

            logger.info(
                "Telemetry stored id=%s mesh=%s device=%s type=%s",
                record_id,
                message.data_mesh_id,
                message.registered_device_id,
                message.device_type,
            )

            await websocket.send_json(
                {
                    "status": "stored",
                    "record_id": record_id,
                    "data_mesh_id": message.data_mesh_id,
                    "registered_device_id": message.registered_device_id,
                    "timestamp": message.timestamp.isoformat(),
                }
            )

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected: %s", client)
