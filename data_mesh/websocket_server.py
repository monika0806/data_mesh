"""WebSocket ingestion endpoint for the IoMT demo.

One service accepts telemetry from all configured devices, validates each
message against the development device registry, and stores accepted readings
in PostgreSQL.

Production deployments should use an authoritative registration service,
authenticate device senders, and manage database credentials securely.
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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
    """Return device ID -> expected mesh/type from the development registry."""

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
    """Create the telemetry table and index if they do not already exist."""

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

    logger.info("PostgreSQL telemetry table is ready")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database connectivity when the server starts."""

    initialize_database()
    yield


app = FastAPI(
    title="IoMT Data Mesh Ingestion",
    version="1.2.0",
    lifespan=lifespan,
)


def save_telemetry(message: TelemetryMessage) -> int:
    """Persist one validated reading and return its database record ID."""

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


@app.get("/")
def home():
    return {
        "message": "IoMT Data Mesh WebSocket Server is running",
        "websocket": "/ws",
        "configured_devices": len(DEVICE_REGISTRY),
        "data_mesh_ids": sorted(
            {device["data_mesh_id"] for device in DEVICE_REGISTRY.values()}
        ),
    }


@app.get("/health")
def health():
    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=3,
        ) as connection:
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
        return {
            "status": "degraded",
            "database": "disconnected",
            "configured_devices": len(DEVICE_REGISTRY),
        }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
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

            registered = DEVICE_REGISTRY.get(
                message.registered_device_id
            )

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