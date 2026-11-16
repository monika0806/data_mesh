
import os
import sys
import json
import uuid
import base64
import secrets
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv

ROOT = Path.cwd()
BACKEND = ROOT.parent / "IOMT_DEVICE_CLIENT" / "server"

load_dotenv(BACKEND.parent / ".env", override=True)
sys.path.insert(0, str(BACKEND))

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi.testclient import TestClient

from session_service import create_session, revoke_session
from data_mesh.secure_session import get_db_connection
import data_mesh.websocket_server as ws_module

DEVICE_RECORD_ID = 1
session_id = None
original_save = ws_module.save_telemetry

try:
    # Find the registered device.
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT registered_device_id, device_type
                FROM public.devices
                WHERE device_record_id = %s
                  AND LOWER(status) = 'active'
            """, (DEVICE_RECORD_ID,))
            device = cur.fetchone()
    finally:
        conn.close()

    if not device:
        raise RuntimeError("Active test device not found")

    registered_id, device_type = device

    # Generate X25519 client keys.
    client_private = X25519PrivateKey.generate()
    client_public = client_private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )

    # Establish a real session in PostgreSQL.
    session = create_session(
        DEVICE_RECORD_ID,
        base64.b64encode(client_public).decode("ascii"),
    )
    session_id = str(session["session_id"])

    server_public = base64.b64decode(
        session["server_public_key"],
        validate=True,
    )

    shared_secret = client_private.exchange(
        X25519PublicKey.from_public_bytes(server_public)
    )

    transcript = (
        b"iomt-session-v1:"
        + session_id.encode("ascii")
        + b":"
        + str(DEVICE_RECORD_ID).encode("ascii")
        + b":"
        + client_public
        + b":"
        + server_public
    )

    session_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=transcript,
    ).derive(shared_secret)

    # Encrypt the complete telemetry message.
    payload = {
        "data_mesh_id": str(uuid.uuid4()),
        "registered_device_id": registered_id,
        "device_type": device_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "telemetry": {
            "heart_rate": 72,
            "battery": 95,
            "test": True,
        },
    }

    nonce = secrets.token_bytes(12)
    ciphertext = AESGCM(session_key).encrypt(
        nonce,
        json.dumps(payload).encode("utf-8"),
        session_id.encode("ascii"),
    )

    envelope = {
        "session_id": session_id,
        "registered_device_id": registered_id,
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
    }

    # Mock only Blob Storage. All authentication and DB checks remain real.
    ws_module.save_telemetry = lambda message, product: "TEST-MOCK-BLOB"

    with TestClient(ws_module.app) as client:
        with client.websocket_connect("/ws") as socket:
            socket.send_json(envelope)
            first = socket.receive_json()

            print("First WebSocket response:", first["status"])
            if first["status"] != "stored":
                raise RuntimeError("Encrypted WebSocket delivery failed")

            socket.send_json(envelope)
            second = socket.receive_json()

            print("Repeated message response:", second["status"])
            if (
                second["status"] != "rejected"
                or second.get("reason")
                != "invalid_or_unauthenticated_message"
            ):
                raise RuntimeError("WebSocket replay was not rejected")

    print("Encrypted WebSocket delivery: SUCCESS")
    print("WebSocket replay protection: SUCCESS")

finally:
    ws_module.save_telemetry = original_save

    if session_id:
        if revoke_session(session_id, DEVICE_RECORD_ID):
            print("Temporary session revoked: SUCCESS")
        else:
            print("WARNING: Temporary session revocation not confirmed")
