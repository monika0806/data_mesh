"""Single-process IoMT simulator for multiple registered devices.

Run from the repository root:
    python -m pip install -r requirements.txt
    python -m uvicorn data_mesh.websocket_server:app --host 127.0.0.1 --port 8000
    python simulator/simulator.py

Set IOMT_WS_URL to change the WebSocket endpoint and
IOMT_REGISTRY_PATH to change the development registry file.

No user IDs are invented here: the backend resolves
registered_device_id to the authorized user.
"""

import asyncio
import json
import logging
import os
import random
import time
from pathlib import Path

import websockets


# --------------------------------------------------
# Logging and configuration
# --------------------------------------------------

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


# --------------------------------------------------
# Load registered devices
# --------------------------------------------------

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

    supported_types = {
        "smart_watch",
        "bp_monitor",
        "glucose_monitor",
        "pulse_oximeter",
        "ecg_monitor",
    }

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
                f"Device fields must be non-empty strings: {device}"
            )

        device_id = device["registered_device_id"]

        if device_id in seen:
            raise RuntimeError(
                f"Duplicate registered_device_id: {device_id}"
            )

        if device["device_type"] not in supported_types:
            raise RuntimeError(
                f"Unsupported device_type for {device_id}: "
                f"{device['device_type']}"
            )

        interval = device.get("telemetry_interval_seconds", 5)

        if (
            isinstance(interval, bool)
            or not isinstance(interval, (int, float))
            or interval <= 0
        ):
            raise RuntimeError(
                f"Invalid telemetry interval for {device_id}"
            )

        device.setdefault("telemetry_interval_seconds", 5)

        seen.add(device_id)

    return devices


# --------------------------------------------------
# Generate synthetic IoMT telemetry
# --------------------------------------------------

def generate_telemetry(device_type):
    """Generate plausible demo values, not medical advice."""

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
        "telemetry": generate_telemetry(
            device["device_type"]
        ),
    }


# --------------------------------------------------
# Simulator
# --------------------------------------------------

async def run_simulator():

    devices = load_devices()

    logger.info(
        "Loaded %d registered-device configurations from %s",
        len(devices),
        REGISTRY_PATH,
    )

    logger.info(
        "Connecting to %s",
        WS_URL,
    )

    # Reconnect if the WebSocket connection is interrupted.
    while True:

        try:

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:

                logger.info(
                    "Connected; simulating %d devices",
                    len(devices),
                )

                while True:

                    cycle_started = time.monotonic()

                    # Send one message per configured device.
                    for device in devices:

                        device_id = device["registered_device_id"]

                        message = make_message(device)

                        logger.info(

    "GENERATED | Device: %s | Type: %s | Values: %s",

    device["registered_device_id"],

    device["device_type"],

    json.dumps(message["telemetry"]),

)
                        await websocket.send(

    json.dumps(message)

)

                        # Read exactly ONE acknowledgement per message.
                        raw_ack = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=10,
                        )

                        ack = json.loads(raw_ack)

                        # The server acknowledges successful PostgreSQL
                        # inserts using status="stored".
                        if (
                            ack.get("status") == "stored"
                            and ack.get("registered_device_id") == device["registered_device_id"]
                        ):

                            logger.info(

        "STORED | Device: %s | Record ID: %s",

        device["registered_device_id"],

        ack.get("record_id"),

    )

                        else:

                            logger.warning(
                                "Server did not confirm storage "
                                "for: %s",
                                ack,
                            )

                    # Repeat the cycle approximately every five seconds.
                    # The minimum configured device interval is respected.
                    cycle_interval = max(
                        float(
                            device.get(
                                "telemetry_interval_seconds",
                                5,
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
                "Connection/communication issue: %s. "
                "Retrying in 3 seconds",
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