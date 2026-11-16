"""Secure IoMT device client: Azure authentication + encrypted WebSocket."""

import asyncio
import base64
import getpass
import json
import logging
import os
import time
import uuid
from pathlib import Path
import requests
import websockets
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


logger = logging.getLogger("secure-iomt-client")

BACKEND_URL = os.getenv(
    "IOMT_BACKEND_URL",
    "https://iomt-backend-app.azurewebsites.net",
).rstrip("/")

WS_URL = os.getenv("IOMT_WS_URL", "")

DEVICE_ID = os.getenv(
    "IOMT_DEVICE_ID",
    "IOMT-MEDICAL-23F8F9",
)


class SecureDeviceClient:

    def __init__(self, device_id):
        self.device_id = device_id
        self.http = requests.Session()

        self.token = None
        self.device_record_id = None
        self.session_id = None
        self.session_key = None
        self.websocket = None
        self.expires_at = None

    def authenticate(self):
        """Authenticate the registered device with the Flask backend."""
        if not BACKEND_URL.startswith("https://"):
            raise RuntimeError(
                "Remote Data Mesh connections must use HTTPS"
            )

        password = getpass.getpass(
            f"Password for device {self.device_id}: "
        )

        if not password:
            raise RuntimeError("Device password cannot be empty")

        response = self.http.post(
            f"{BACKEND_URL}/api/auth/device-login",
            json={
                "device_id": self.device_id,
                "password": password,
            },
            timeout=20,
        )

        # Do not retain the password after authentication.
        password = None

        if not response.ok:
            raise RuntimeError(
                f"Device authentication failed: HTTP {response.status_code}"
            )

        result = response.json()

        self.token = result["token"]
        device = result["device"]

        self.device_record_id = int(device["device_record_id"])

        if device["registered_device_id"] != self.device_id:
            raise RuntimeError("Authenticated device ID mismatch")

        if str(device["status"]).lower() != "active":
            raise RuntimeError("Device is not active")

        logger.info(
            "Authenticated device record=%s type=%s",
            self.device_record_id,
            device.get("device_type"),
        )

        return device

    def establish_session(self):
        """Perform X25519 exchange and derive the AES-256 session key."""

        private_key = X25519PrivateKey.generate()

        client_public_bytes = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

        client_public_b64 = base64.b64encode(
            client_public_bytes
        ).decode("ascii")

        response = self.http.post(
            f"{BACKEND_URL}/api/device/session",
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            json={
                "client_public_key": client_public_b64,
            },
            timeout=20,
        )

        if not response.ok:
            raise RuntimeError(
                f"Session creation failed: HTTP {response.status_code}"
            )

        result = response.json()

        self.session_id = result["session_id"]

        server_public_bytes = base64.b64decode(
            result["server_public_key"],
            validate=True,
        )

        server_public_key = X25519PublicKey.from_public_bytes(
            server_public_bytes
        )

        shared_secret = private_key.exchange(server_public_key)

        transcript = (
            b"iomt-session-v1:"
            + self.session_id.encode("ascii")
            + b":"
            + str(self.device_record_id).encode("ascii")
            + b":"
            + client_public_bytes
            + b":"
            + server_public_bytes
        )

        self.session_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=transcript,
        ).derive(shared_secret)

        expires_in = int(result.get("expires_in", 900))
        self.expires_at = time.monotonic() + expires_in

        logger.info(
            "Secure session established; expires in %s seconds",
            expires_in,
        )

    def encrypt_message(self, message):
        """Encrypt one telemetry message using AES-256-GCM."""

        if not self.session_key or not self.session_id:
            raise RuntimeError("Secure session is not established")

        if time.monotonic() >= self.expires_at:
            raise RuntimeError("Session expired; establish a new session")

        if message.get("registered_device_id") != self.device_id:
            raise ValueError("Telemetry device ID mismatch")

        nonce = os.urandom(12)

        ciphertext = AESGCM(self.session_key).encrypt(
            nonce,
            json.dumps(
                message,
                separators=(",", ":"),
            ).encode("utf-8"),
            self.session_id.encode("ascii"),
        )

        return {
            "session_id": self.session_id,
            "registered_device_id": self.device_id,
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        }

    async def connect(self):
        """Connect to the deployed secure Data Mesh WebSocket."""

        if not WS_URL:
            raise RuntimeError(
                "Set IOMT_WS_URL to the deployed Data Mesh wss:// endpoint"
            )

        if not WS_URL.startswith("wss://"):
            raise RuntimeError(
                "Remote Data Mesh connections must use wss://"
            )

        self.websocket = await websockets.connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
        )

        logger.info("Connected to secure Data Mesh")

    async def send_telemetry(self, message):
        """Encrypt, send and receive the storage acknowledgement."""

        if self.websocket is None:
            raise RuntimeError("WebSocket is not connected")

        envelope = self.encrypt_message(message)

        await self.websocket.send(json.dumps(envelope))

        raw_ack = await asyncio.wait_for(
            self.websocket.recv(),
            timeout=20,
        )

        ack = json.loads(raw_ack)

        if (
            ack.get("status") == "stored"
            and ack.get("registered_device_id") == self.device_id
        ):
            logger.info(
                "Telemetry stored successfully: %s",
                ack.get("blob_name", "Blob confirmed"),
            )
            return ack

        logger.error(
            "Telemetry was not stored; server status=%s reason=%s",
            ack.get("status"),
            ack.get("reason"),
        )

        raise RuntimeError("Data Mesh did not confirm telemetry storage")

    async def close(self):
        """Close WebSocket and revoke the temporary session."""

        try:
            if self.websocket is not None:
                try:
                    await self.websocket.close()
                    logger.info("WebSocket connection closed")
                except Exception:
                    logger.warning(
                        "WebSocket close failed during shutdown",
                        exc_info=True,
                    )
                finally:
                    self.websocket = None

            if self.session_id and self.token:
                try:
                    response = self.http.delete(
                        f"{BACKEND_URL}/api/device/session/{self.session_id}",
                        headers={
                            "Authorization": f"Bearer {self.token}",
                        },
                        timeout=15,
                    )

                    if response.ok:
                        logger.info("Temporary session revoked")
                    else:
                        logger.warning(
                            "Session revocation returned HTTP %s",
                            response.status_code,
                        )

                except requests.RequestException:
                    logger.warning(
                        "Could not revoke session during shutdown"
                    )

        finally:
            self.session_key = None
            self.token = None
            self.session_id = None
            self.expires_at = None
            self.http.close()


async def main():
    client = SecureDeviceClient(DEVICE_ID)

    try:
        device = await asyncio.to_thread(client.authenticate)

        if str(device.get("device_type", "")).strip().lower() not in (
            "bp_monitor",
            "blood pressure monitor",
            "blood_pressure_monitor",
        ):
            raise RuntimeError("This test client is configured for a BP monitor")

        await asyncio.to_thread(client.establish_session)
        await client.connect()

        message = {
            "data_mesh_id": "MESH001",
            "registered_device_id": DEVICE_ID,
            "device_type": "bp_monitor",
            "timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(),
            ),
            "telemetry": {
                "systolic": 118,
                "diastolic": 76,
                "pulse": 72,
                "battery": 98,
                "test": True,
            },
        }

        await client.send_telemetry(message)

    finally:
        await client.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Client stopped")
    except Exception as exc:
        logger.error("Secure client failed: %s", exc)
        raise SystemExit(1)
