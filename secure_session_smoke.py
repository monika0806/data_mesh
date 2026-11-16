import sys
import json
import base64
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
BACKEND = ROOT.parent / "IOMT_DEVICE_CLIENT" / "server"

load_dotenv(BACKEND.parent / ".env")
sys.path.insert(0, str(BACKEND))

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
)
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from session_service import create_session, revoke_session
from data_mesh.secure_session import (
    get_db_connection,
    decrypt_and_consume,
)

DEVICE_RECORD_ID = 1
session_id = None

try:
    # Get the authoritative registered device ID.
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT registered_device_id
                FROM public.devices
                WHERE device_record_id = %s
                  AND LOWER(status) = 'active'
                """,
                (DEVICE_RECORD_ID,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise RuntimeError("Test device is not active")

    registered_device_id = row[0]

    # Generate client X25519 keys.
    client_private = X25519PrivateKey.generate()
    client_public = client_private.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )

    # Create a temporary session using the existing backend service.
    session = create_session(
        DEVICE_RECORD_ID,
        base64.b64encode(client_public).decode("ascii"),
    )
    session_id = str(session["session_id"])

    server_public = base64.b64decode(
        session["server_public_key"],
        validate=True,
    )

    # Reproduce the exact backend HKDF transcript.
    shared_secret = client_private.exchange(
        __import__(
            "cryptography.hazmat.primitives.asymmetric.x25519",
            fromlist=["X25519PublicKey"],
        ).X25519PublicKey.from_public_bytes(server_public)
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

    # Encrypt harmless sample telemetry.
    payload = json.dumps({
        "heart_rate": 72,
        "battery": 95,
        "test": True,
    }).encode("utf-8")

    nonce = __import__("secrets").token_bytes(12)
    ciphertext = AESGCM(session_key).encrypt(
        nonce,
        payload,
        session_id.encode("ascii"),
    )

    # First delivery must decrypt successfully.
    plaintext = decrypt_and_consume(
        session_id,
        registered_device_id,
        nonce,
        ciphertext,
    )

    if plaintext != payload:
        raise RuntimeError("Decrypted payload did not match")

    print("First decryption: SUCCESS")

    # Reusing the same nonce must be rejected.
    try:
        decrypt_and_consume(
            session_id,
            registered_device_id,
            nonce,
            ciphertext,
        )
        raise RuntimeError("Replay was incorrectly accepted")
    except ValueError as exc:
        if str(exc) != "Replay detected":
            raise
        print("Replay protection: SUCCESS")

finally:
    if session_id:
        if revoke_session(session_id, DEVICE_RECORD_ID):
            print("Temporary session revoked: SUCCESS")
        else:
            print("Temporary session revocation: NOT CONFIRMED")
'PY'
