"""IoMT WebSocket ingestion with PostgreSQL registration checks and Blob persistence.

PostgreSQL is used only as the authoritative registration source. Telemetry is
persisted as one JSON blob per event in a private Azure Blob Storage container.
"""

import json
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient, ContentSettings
from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field, ValidationError


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("iomt-data-mesh")


# Use the existing database URL when the optional purpose-specific variable
# is not configured. No database URL needs to be changed.
REGISTRATION_DATABASE_URL = (
    os.getenv("IOMT_REGISTRATION_DATABASE_URL")
    or os.getenv("IOMT_DATABASE_URL")
)

AZURE_STORAGE_CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING"
)

TELEMETRY_CONTAINER = os.getenv(
    "IOMT_TELEMETRY_CONTAINER",
    "iomt-telemetry",
)

# Actual PostgreSQL inspection confirmed that all five current registrations
# have status "active". Other statuses are allowed only if explicitly configured.
ALLOWED_DEVICE_STATUSES = {
    value.strip().lower()
    for value in os.getenv(
        "IOMT_ALLOWED_DEVICE_STATUSES",
        "active,registered,enabled",
    ).split(",")
    if value.strip()
}


# Authoritative database device_type -> logical Data Mesh product.
#
# Required product types:
#   smart_watch
#   bp_monitor
#   glucose_monitor
#   ecg_monitor
#   pulse_oximeter
#
# "glucometer" is included because the existing registration database uses
# the device_type "Glucometer" for three currently registered devices.
PRODUCT_TYPE_ALIASES = {
    # Product 1: smartwatch
    "smartwatch": "product-1",
    "smart_watch": "product-1",
    "smart watch": "product-1",

    # Product 2: blood pressure monitor
    "blood_pressure": "product-2",
    "blood_pressure_monitor": "product-2",
    "bp_monitor": "product-2",
    "blood pressure monitor": "product-2",

    # Product 3: glucose monitor
    "glucose": "product-3",
    "glucometer": "product-3",
    "glucose_monitor": "product-3",
    "glucose monitor": "product-3",

    # Product 4: ECG monitor
    "ecg": "product-4",
    "ecg_monitor": "product-4",
    "ecg monitor": "product-4",

    # Product 5: pulse oximeter
    "pulse_oximeter": "product-5",
    "pulse oximeter": "product-5",
}


PRODUCT_NAMES = {
    "product-1": "smartwatch-readings",
    "product-2": "blood-pressure-readings",
    "product-3": "glucose-readings",
    "product-4": "ecg-readings",
    "product-5": "pulse-oximeter-readings",
}


class TelemetryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_mesh_id: str = Field(min_length=1)
    registered_device_id: str = Field(min_length=1)
    device_type: str = Field(min_length=1)
    timestamp: datetime
    telemetry: dict[str, Any]


def canonical_type(value: str) -> str:
    """Normalize capitalization, spaces, and hyphens in device types."""
    return re.sub(r"[\s-]+", "_", value.strip().lower())


NORMALIZED_PRODUCT_TYPE_ALIASES = {
    canonical_type(alias): product_id
    for alias, product_id in PRODUCT_TYPE_ALIASES.items()
}


def product_for_type(device_type: str) -> str | None:
    """Return the logical product for a supported device type."""
    return NORMALIZED_PRODUCT_TYPE_ALIASES.get(
        canonical_type(device_type)
    )


def get_registered_device(
    registered_device_id: str,
) -> dict[str, Any] | None:
    """Read authoritative registration details for a device."""
    if not REGISTRATION_DATABASE_URL:
        raise RuntimeError(
            "Neither IOMT_DATABASE_URL nor "
            "IOMT_REGISTRATION_DATABASE_URL is configured"
        )

    # Read-only SELECT. No registration records are created or modified.
    with psycopg2.connect(
        REGISTRATION_DATABASE_URL,
        connect_timeout=5,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT registered_device_id, device_type, status
                FROM public.devices
                WHERE registered_device_id = %s
                LIMIT 1
                """,
                (registered_device_id,),
            )
            row = cur.fetchone()

    if row is None:
        return None

    return {
        "registered_device_id": row[0],
        "device_type": row[1],
        "status": row[2],
    }


def get_blob_container():
    """Return a client for the configured telemetry container."""
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING is not configured"
        )

    service = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    return service.get_container_client(TELEMETRY_CONTAINER)


def save_telemetry(
    message: TelemetryMessage,
    product_id: str,
) -> str:
    """Write one immutable JSON event to Blob Storage."""
    container = get_blob_container()

    received_at = datetime.now(timezone.utc)
    event_id = str(uuid.uuid4())

    # Registered IDs are used as path components. Reject path separators
    # to prevent a device ID from creating unexpected blob paths.
    safe_device_id = message.registered_device_id

    if (
        "/" in safe_device_id
        or "\\" in safe_device_id
        or safe_device_id in {".", ".."}
    ):
        raise ValueError("Invalid registered device ID for blob path")

    device_timestamp = message.timestamp
    if device_timestamp.tzinfo is None:
        # Treat a timestamp without timezone as UTC.
        device_timestamp = device_timestamp.replace(tzinfo=timezone.utc)
    else:
        device_timestamp = device_timestamp.astimezone(timezone.utc)

    blob_name = (
        f"{PRODUCT_NAMES[product_id]}/"
        f"{safe_device_id}/"
        f"{device_timestamp:%Y/%m/%d}/"
        f"{received_at:%H%M%S%f}-{event_id}.json"
    )

    record = {
        "event_id": event_id,
        "data_mesh_id": message.data_mesh_id,
        "registered_device_id": message.registered_device_id,
        "device_type": message.device_type,
        "product_id": product_id,
        "device_timestamp": device_timestamp.isoformat(),
        "received_at": received_at.isoformat(),
        "telemetry": message.telemetry,
    }

    blob = container.get_blob_client(blob_name)

    blob.upload_blob(
        json.dumps(
            record,
            separators=(",", ":"),
            default=str,
        ),
        overwrite=False,
        content_settings=ContentSettings(
            content_type="application/json"
        ),
    )

    return blob_name


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Deliberately do not create or write telemetry tables in PostgreSQL.
    yield


app = FastAPI(
    title="IoMT Data Mesh Ingestion",
    version="2.1.0",
    lifespan=lifespan,
)


@app.get("/")
def home():
    return {
        "message": "IoMT Data Mesh WebSocket Server is running",
        "websocket": "/ws",
        "telemetry_storage": "azure-blob-storage",
        "telemetry_container": TELEMETRY_CONTAINER,
        "registration_source": "public.devices",
        "configured_product_types": sorted(PRODUCT_TYPE_ALIASES),
    }


@app.get("/health")
def health():
    status = {
        "status": "ok",
        "registration_database": "not_checked",
        "blob_storage": "not_checked",
    }

    missing_configuration = [
        name
        for name, value in [
            ("IOMT_DATABASE_URL", REGISTRATION_DATABASE_URL),
            (
                "AZURE_STORAGE_CONNECTION_STRING",
                AZURE_STORAGE_CONNECTION_STRING,
            ),
        ]
        if not value
    ]

    if missing_configuration:
        return {
            "status": "degraded",
            **status,
            "missing_configuration": missing_configuration,
        }

    try:
        with psycopg2.connect(
            REGISTRATION_DATABASE_URL,
            connect_timeout=3,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

        status["registration_database"] = "connected"

    except Exception:
        logger.exception("Registration database health check failed")
        status["registration_database"] = "disconnected"
        status["status"] = "degraded"

    try:
        container = get_blob_container()
        container.get_container_properties()
        status["blob_storage"] = "connected"

    except Exception:
        logger.exception("Blob Storage health check failed")
        status["blob_storage"] = "disconnected"
        status["status"] = "degraded"

    return status


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    client = (
        websocket.client.host
        if websocket.client
        else "unknown"
    )

    logger.info("WebSocket client connected: %s", client)

    try:
        while True:
            # Validate incoming telemetry message structure.
            try:
                raw_data = await websocket.receive_json()
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

            # Validate the submitted device type against supported products.
            expected_product = product_for_type(message.device_type)

            if expected_product is None:
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "unsupported_device_type",
                        "registered_device_id": (
                            message.registered_device_id
                        ),
                    }
                )
                continue

            # Verify registration from the authoritative PostgreSQL source.
            try:
                registered = get_registered_device(
                    message.registered_device_id
                )

            except Exception:
                # Fail closed if registration cannot be verified.
                logger.exception(
                    "Registration lookup failed for %s",
                    message.registered_device_id,
                )

                await websocket.send_json(
                    {
                        "status": "error",
                        "reason": "registration_lookup_unavailable",
                    }
                )
                continue

            if registered is None:
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "unknown_registered_device",
                        "registered_device_id": (
                            message.registered_device_id
                        ),
                    }
                )
                continue

            # Only statuses explicitly permitted by configuration are accepted.
            registered_status = str(
                registered.get("status") or ""
            ).strip().lower()

            if registered_status not in ALLOWED_DEVICE_STATUSES:
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "device_status_ineligible",
                        "registered_device_id": (
                            message.registered_device_id
                        ),
                    }
                )
                continue

            # The submitted type must match the authoritative registration.
            registered_product = product_for_type(
                str(registered.get("device_type") or "")
            )

            if (
                registered_product is None
                or registered_product != expected_product
            ):
                await websocket.send_json(
                    {
                        "status": "rejected",
                        "reason": "device_type_mismatch",
                        "registered_device_id": (
                            message.registered_device_id
                        ),
                    }
                )
                continue

            # Persist telemetry in Blob Storage. Acknowledge only after success.
            try:
                blob_name = save_telemetry(
                    message,
                    expected_product,
                )

            except Exception:
                logger.exception(
                    "Blob persistence failed for %s",
                    message.registered_device_id,
                )

                await websocket.send_json(
                    {
                        "status": "error",
                        "reason": "blob_storage_write_failed",
                        "registered_device_id": (
                            message.registered_device_id
                        ),
                    }
                )
                continue

            logger.info(
                "Telemetry stored blob=%s product=%s device=%s",
                blob_name,
                expected_product,
                message.registered_device_id,
            )

            await websocket.send_json(
                {
                    "status": "stored",
                    "storage": "azure-blob-storage",
                    "blob_name": blob_name,
                    "product_id": expected_product,
                    "registered_device_id": (
                        message.registered_device_id
                    ),
                    "timestamp": message.timestamp.isoformat(),
                }
            )

    except WebSocketDisconnect:
        logger.info(
            "WebSocket client disconnected: %s",
            client,
        )

# --------------------------------------------------
# Read-only telemetry API
# --------------------------------------------------

READ_API_KEY = os.getenv("IOMT_READ_API_KEY", "")

PRODUCT_BY_DEVICE_TYPE = {
    "smart_watch": "smartwatch-readings",
    "bp_monitor": "blood-pressure-readings",
    "glucose_monitor": "glucose-readings",
    "pulse_oximeter": "pulse-oximeter-readings",
    "ecg_monitor": "ecg-readings",
}


@app.get("/api/telemetry")
def read_device_telemetry(
    registered_device_id: str = Query(..., min_length=1),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    sort: str = Query(default="newest", pattern="^(newest|oldest)$"),
    limit: int = Query(default=100, ge=1, le=1000),
    x_iomt_read_key: str | None = Header(default=None),
):
    # Never leave the read API unprotected.
    if not READ_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="Telemetry read API is not configured",
        )

    if x_iomt_read_key != READ_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Confirm registration and apply the same status policy used by ingestion.
    try:
        device = get_registered_device(registered_device_id)
    except Exception:
        logger.exception("Device registration lookup failed")
        raise HTTPException(
            status_code=503,
            detail="Registration service unavailable",
        )

    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    device_status = str(device.get("status") or "").strip().lower()
    if device_status not in ALLOWED_DEVICE_STATUSES:
        raise HTTPException(status_code=403, detail="Device status is not allowed")

    product_id = product_for_type(str(device.get("device_type") or ""))
    if not product_id:
        raise HTTPException(status_code=400, detail="Unsupported device type")

    product_name = PRODUCT_NAMES[product_id]

    # Validate optional date boundaries.
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None

        if start_dt and start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        if end_dt and end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)

        if start_dt and end_dt and start_dt > end_dt:
            raise ValueError("start must not be after end")
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail="Use valid ISO dates for start and end",
        )

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = TELEMETRY_CONTAINER

    if not connection_string:
        raise HTTPException(
            status_code=503,
            detail="Telemetry storage is not configured",
        )

    # Blobs are partitioned by product/device/date.
    prefix = f"{product_name}/{registered_device_id}/"
    records = []

    try:
        service = BlobServiceClient.from_connection_string(
            connection_string
        )
        container = service.get_container_client(container_name)

        for blob in container.list_blobs(name_starts_with=prefix):
            blob_client = container.get_blob_client(blob.name)
            record = json.loads(
                blob_client.download_blob().readall()
            )

            timestamp_text = (
                record.get("device_timestamp")
                or record.get("received_at")
            )
            if not timestamp_text:
                continue

            timestamp = datetime.fromisoformat(
                timestamp_text.replace("Z", "+00:00")
            )
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            if start_dt and timestamp < start_dt:
                continue
            if end_dt and timestamp > end_dt:
                continue

            records.append(record)

    except Exception:
        logger.exception("Blob telemetry retrieval failed")
        raise HTTPException(
            status_code=503,
            detail="Unable to retrieve telemetry",
        )

    records.sort(
        key=lambda r: (
            r.get("device_timestamp") or r.get("received_at") or ""
        ),
        reverse=(sort == "newest"),
    )

    return {
        "registered_device_id": registered_device_id,
        "device_type": device_type,
        "count": min(len(records), limit),
        "sort": sort,
        "telemetry": records[:limit],
    }