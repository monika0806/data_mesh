"""IoMT simulator with automatic, read-only PostgreSQL device discovery.

Run from the repository root:
    python simulator/simulator.py

Required environment variables in .env:
    IOMT_REGISTRATION_DATABASE_URL (or IOMT_DATABASE_URL)
    IOMT_WS_URL
    AZURE_STORAGE_CONNECTION_STRING (used by the Data Mesh server)

The simulator reads registrations from public.devices only.
It does not modify PostgreSQL tables, triggers, functions, or registrations.
Telemetry is sent to the existing Data Mesh WebSocket endpoint.
"""

import asyncio
import json
import logging
import os
import random
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import websockets


# --------------------------------------------------
# Logging and configuration
# --------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("iomt-simulator")

WS_URL = os.getenv(
    "IOMT_WS_URL",
    "ws://127.0.0.1:8000/ws",
)

REGISTRY_PATH = Path(
    os.getenv(
        "IOMT_REGISTRY_PATH",
        Path(__file__).resolve().parents[1] / "device_registry.json",
    )
)

REGISTRATION_DATABASE_URL = (
    os.getenv("IOMT_REGISTRATION_DATABASE_URL")
    or os.getenv("IOMT_DATABASE_URL")
)

DEVICE_DISCOVERY_INTERVAL_SECONDS = 30
DEFAULT_TELEMETRY_INTERVAL_SECONDS = 5

# Only these five device types are supported by the dashboard.
SUPPORTED_DEVICE_TYPES = {
    "smart_watch",
    "bp_monitor",
    "glucose_monitor",
    "pulse_oximeter",
    "ecg_monitor",
}

# Normalize names used by PostgreSQL into simulator/dashboard types.
DEVICE_TYPE_ALIASES = {
    "smart watch": "smart_watch",
    "smart_watch": "smart_watch",

    "blood pressure monitor": "bp_monitor",
    "bp monitor": "bp_monitor",
    "bp_monitor": "bp_monitor",

    "glucometer": "glucose_monitor",
    "glucose monitor": "glucose_monitor",
    "glucose_monitor": "glucose_monitor",

    "pulse oximeter": "pulse_oximeter",
    "pulse_oximeter": "pulse_oximeter",

    "ecg monitor": "ecg_monitor",
    "ecg_monitor": "ecg_monitor",
}


# --------------------------------------------------
# Load and validate local device registry
# --------------------------------------------------

def normalize_device_type(raw_type):
    """Return a supported canonical type, or None if unsupported."""

    normalized = str(raw_type or "").strip().lower()
    return DEVICE_TYPE_ALIASES.get(normalized)


def load_devices():
    """Load and validate configured IoMT devices."""

    try:
        payload = json.loads(
            REGISTRY_PATH.read_text(encoding="utf-8")
        )
        devices = payload["devices"]

    except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise RuntimeError(
            f"Could not load device registry at {REGISTRY_PATH}: {exc}"
        ) from exc

    if not isinstance(devices, list) or not devices:
        raise RuntimeError(
            "Registry must contain a non-empty 'devices' list"
        )

    seen = set()

    required = {
        "data_mesh_id",
        "registered_device_id",
        "device_type",
    }

    validated_devices = []

    for device in devices:
        if not isinstance(device, dict):
            raise RuntimeError(
                "Each device entry must be a JSON object"
            )

        if not required.issubset(device):
            raise RuntimeError(
                f"Each device must contain {sorted(required)}"
            )

        if not all(
            isinstance(device[key], str) and device[key].strip()
            for key in required
        ):
            raise RuntimeError(
                "Device fields must be non-empty strings"
            )

        device_id = device["registered_device_id"].strip()
        device_type = normalize_device_type(device["device_type"])

        if device_id in seen:
            raise RuntimeError(
                f"Duplicate registered_device_id: {device_id}"
            )

        # Keep local registry entries aligned with the five dashboard types.
        if device_type not in SUPPORTED_DEVICE_TYPES:
            logger.warning(
                "Ignoring unsupported registry device: %s (type=%s)",
                device_id,
                device["device_type"],
            )
            continue

        interval = device.get(
            "telemetry_interval_seconds",
            DEFAULT_TELEMETRY_INTERVAL_SECONDS,
        )

        if (
            isinstance(interval, bool)
            or not isinstance(interval, (int, float))
            or interval <= 0
        ):
            raise RuntimeError(
                f"Invalid telemetry interval for {device_id}"
            )

        validated_devices.append({
            **device,
            "registered_device_id": device_id,
            "device_type": device_type,
            "telemetry_interval_seconds": float(interval),
        })
        seen.add(device_id)

    if not validated_devices:
        raise RuntimeError(
            "Registry contains no devices with supported device types"
        )

    return validated_devices


# --------------------------------------------------
# Read-only PostgreSQL device discovery
# --------------------------------------------------

def discover_registered_devices():
    """Read active supported registrations without modifying PostgreSQL."""

    if not REGISTRATION_DATABASE_URL:
        raise RuntimeError(
            "Database URL is missing. Configure "
            "IOMT_REGISTRATION_DATABASE_URL in .env."
        )

    discovered = []

    # This connection executes SELECT only.
    with psycopg2.connect(
        REGISTRATION_DATABASE_URL,
        connect_timeout=10,
    ) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT registered_device_id, device_type, status
                FROM public.devices
                WHERE LOWER(status) = 'active'
                ORDER BY registered_device_id
                """
            )

            for row in cur.fetchall():
                device_id = str(
                    row["registered_device_id"] or ""
                ).strip()

                device_type = normalize_device_type(
                    row["device_type"]
                )

                if not device_id:
                    continue

                if device_type not in SUPPORTED_DEVICE_TYPES:
                    logger.info(
                        "Ignoring unsupported registered device: %s "
                        "(type=%s)",
                        device_id,
                        row["device_type"],
                    )
                    continue

                discovered.append({
                    "data_mesh_id": os.getenv(
                        "IOMT_DATA_MESH_ID",
                        "MESH001",
                    ),
                    "registered_device_id": device_id,
                    "device_type": device_type,
                    "telemetry_interval_seconds":
                        DEFAULT_TELEMETRY_INTERVAL_SECONDS,
                })

    return discovered


# --------------------------------------------------
# Generate synthetic IoMT telemetry
# --------------------------------------------------

def generate_telemetry(device_type):
    """Generate demo values, not medical advice."""

    if device_type == "smart_watch":
        return {
            "heart_rate": random.randint(70, 76),
            "spo2": random.randint(97, 99),
            "temperature": round(random.uniform(36.5, 36.8), 1),
            "battery": random.randint(95, 100),
        }

    if device_type == "bp_monitor":
        return {
            "systolic": random.randint(115, 120),
            "diastolic": random.randint(72, 78),
            "pulse": random.randint(70, 76),
            "battery": random.randint(95, 100),
        }

    if device_type == "glucose_monitor":
        return {
            "glucose": random.randint(90, 100),
            "measurement_type": "fasting",
            "battery": random.randint(95, 100),
        }

    if device_type == "pulse_oximeter":
        return {
            "spo2": random.randint(97, 99),
            "heart_rate": random.randint(70, 76),
            "battery": random.randint(95, 100),
        }

    if device_type == "ecg_monitor":
        return {
            "heart_rate": random.randint(70, 76),
            "ecg_status": "normal",
            "battery": random.randint(95, 100),
        }

    raise ValueError(
        f"Unsupported device_type: {device_type}"
    )


# --------------------------------------------------
# Construct telemetry message
# --------------------------------------------------

def make_message(device):
    """Build a telemetry message for a registered device."""

    return {
        "data_mesh_id": device["data_mesh_id"],
        "registered_device_id": device["registered_device_id"],
        "device_type": device["device_type"],
        "timestamp": time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(),
        ),
        "telemetry": generate_telemetry(device["device_type"]),
    }


# --------------------------------------------------
# Simulator
# --------------------------------------------------

async def run_simulator():
    """Simulate devices and discover new active registrations every 30s."""

    devices = load_devices()

    known_device_ids = {
        device["registered_device_id"]
        for device in devices
    }

    next_discovery_at = 0.0

    logger.info(
        "Loaded %d supported device configurations from %s",
        len(devices),
        REGISTRY_PATH,
    )

    logger.info("Connecting to %s", WS_URL)

    # Reconnect if the WebSocket connection is interrupted.
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:

                logger.info("Connected to Data Mesh WebSocket")

                while True:
                    cycle_started = time.monotonic()

                    # Discover new registrations every 30 seconds.
                    if cycle_started >= next_discovery_at:
                        try:
                            discovered_devices = await asyncio.to_thread(
                                discover_registered_devices
                            )

                            added_count = 0

                            for new_device in discovered_devices:
                                device_id = (
                                    new_device["registered_device_id"]
                                )

                                if device_id not in known_device_ids:
                                    devices.append(new_device)
                                    known_device_ids.add(device_id)
                                    added_count += 1

                                    logger.info(
                                        "NEW DEVICE | ID: %s | Type: %s",
                                        device_id,
                                        new_device["device_type"],
                                    )

                            logger.info(
                                "Discovery complete | "
                                "Known devices: %d | Newly added: %d",
                                len(devices),
                                added_count,
                            )

                        except Exception:
                            logger.exception(
                                "Device discovery failed; "
                                "continuing with known devices"
                            )

                        next_discovery_at = (
                            time.monotonic()
                            + DEVICE_DISCOVERY_INTERVAL_SECONDS
                        )

                    if not devices:
                        await asyncio.sleep(5)
                        continue

                    # Send one message per configured/discovered device.
                    for device in list(devices):
                        device_id = device["registered_device_id"]
                        message = make_message(device)

                        logger.info(
                            "GENERATED | Device: %s | Type: %s | Values: %s",
                            device_id,
                            device["device_type"],
                            json.dumps(message["telemetry"]),
                        )

                        await websocket.send(json.dumps(message))

                        # Read exactly one acknowledgement per message.
                        raw_ack = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=10,
                        )

                        ack = json.loads(raw_ack)

                        if (
                            ack.get("status") == "stored"
                            and ack.get("registered_device_id") == device_id
                        ):
                            logger.info(
                                "STORED | Device: %s | Blob: %s",
                                device_id,
                                ack.get("blob_name", "not provided"),
                            )
                        else:
                            logger.warning(
                                "Storage not confirmed for %s: %s",
                                device_id,
                                ack,
                            )

                    # Use the shortest configured interval so devices
                    # configured for faster telemetry are not delayed.
                    cycle_interval = min(
                        float(
                            device.get(
                                "telemetry_interval_seconds",
                                DEFAULT_TELEMETRY_INTERVAL_SECONDS,
                            )
                        )
                        for device in devices
                    )

                    elapsed = time.monotonic() - cycle_started

                    await asyncio.sleep(
                        max(0, cycle_interval - elapsed)
                    )

        except (
            OSError,
            asyncio.TimeoutError,
            websockets.exceptions.WebSocketException,
            json.JSONDecodeError,
        ) as exc:
            logger.warning(
                "Connection/communication issue: %s. Retrying in 3 seconds",
                exc,
            )
            await asyncio.sleep(3)


# --------------------------------------------------
# Entry point
# --------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(run_simulator())

    except KeyboardInterrupt:
        logger.info("Simulator stopped")

    except RuntimeError as exc:
        logger.error("Simulator configuration error: %s", exc)
        raise SystemExit(1)