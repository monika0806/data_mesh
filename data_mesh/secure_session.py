"""Server-side IoMT session decryption and replay protection."""

import base64
import logging
import os
import uuid
from pathlib import Path

import psycopg2
from psycopg2.errors import UniqueViolation
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import load_dotenv

logger = logging.getLogger("iomt-secure-session")

# Load the existing backend .env for local development.
# In Azure, use App Service environment settings instead.
BACKEND_ENV = (
    Path(__file__).resolve().parents[2]
    / "IOMT_DEVICE_CLIENT"
    / ".env"
)
if BACKEND_ENV.is_file():
    load_dotenv(BACKEND_ENV)



def get_db_connection():
    """Connect using the same PostgreSQL configuration as Data Mesh."""

    database_url = (
        os.getenv("IOMT_REGISTRATION_DATABASE_URL")
        or os.getenv("IOMT_DATABASE_URL")
    )

    if database_url:
        return psycopg2.connect(
            database_url,
            connect_timeout=10,
            sslmode=os.getenv("DB_SSLMODE", "require"),
        )

    # Local development fallback.
    required = ("DB_HOST", "DB_NAME", "DB_USER")
    missing = [name for name in required if not os.getenv(name)]

    if missing:
        raise RuntimeError(
            "Database configuration missing: " + ", ".join(missing)
        )

    options = {
        "host": os.environ["DB_HOST"],
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "sslmode": os.getenv("DB_SSLMODE", "require"),
        "connect_timeout": 10,
    }

    password = os.getenv("DB_PASSWORD")
    if password:
        options["password"] = password

    return psycopg2.connect(**options)




def get_master_key():
    """Load the existing 32-byte session encryption key."""
    encoded = os.getenv("IOMT_SESSION_ENCRYPTION_KEY")
    if not encoded:
        raise RuntimeError("Session encryption key is not configured")

    normalized = encoded.replace("-", "+").replace("_", "/")

    try:
        key = base64.b64decode(normalized, validate=True)
    except Exception as exc:
        raise RuntimeError("Invalid session encryption key") from exc

    if len(key) != 32:
        raise RuntimeError("Session encryption key must be 32 bytes")

    return key


def session_aad(session_id, device_record_id):
    return (
        b"iomt-session-store-v1:"
        + str(session_id).encode("ascii")
        + b":"
        + str(int(device_record_id)).encode("ascii")
    )


def audit_replay(device_id, session_id):
    """Record a replay rejection without exposing telemetry or keys."""
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO public.security_audit_logs
                (event_type, status, device_id, reason)
            VALUES (%s, %s, %s, %s)
            """,
            (
                "REPLAY_DETECTED",
                "DENIED",
                device_id,
                f"Duplicate session nonce for session {session_id}",
            ),
        )
        connection.commit()
    except Exception:
        if connection:
            connection.rollback()
        logger.exception("Unable to write replay audit event")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def decrypt_and_consume(
    session_id,
    registered_device_id,
    nonce,
    ciphertext,
    validated_plaintext=None,
):
    """
    Authenticate, decrypt, and atomically consume one telemetry nonce.

    Returns the authenticated plaintext bytes.
    """
    session_id = str(uuid.UUID(str(session_id)))
    nonce = bytes(nonce)
    ciphertext = bytes(ciphertext)

    if len(nonce) != 12:
        raise ValueError("Invalid payload nonce")

    if len(ciphertext) < 16:
        raise ValueError("Invalid encrypted payload")

    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Resolve the device and lock its session for the transaction.
        cursor.execute(
            """
            SELECT
                d.device_record_id,
                d.status,
                s.encrypted_session_key,
                s.key_nonce
            FROM public.devices d
            JOIN public.device_sessions s
              ON s.device_record_id = d.device_record_id
            WHERE d.registered_device_id = %s
              AND s.session_id = %s
              AND s.expires_at > CURRENT_TIMESTAMP
              AND s.revoked_at IS NULL
            FOR UPDATE OF s
            """,
            (registered_device_id, session_id),
        )

        row = cursor.fetchone()

        if not row:
            raise ValueError("Session expired or invalid")

        device_record_id, device_status, encrypted_key, storage_nonce = row

        if str(device_status or "").strip().lower() not in {
            "active", "registered", "enabled"
        }:
            raise ValueError("Device is not permitted")

        session_key = AESGCM(get_master_key()).decrypt(
            bytes(storage_nonce),
            bytes(encrypted_key),
            session_aad(session_id, device_record_id),
        )

        # Authenticate and decrypt before accepting the nonce.
        plaintext = AESGCM(session_key).decrypt(
            nonce,
            ciphertext,
            session_id.encode("ascii"),
        )

        # Validate authenticated plaintext before consuming its nonce.
        if validated_plaintext is not None:
            validated_plaintext(plaintext)

        cursor.execute(
            """
            INSERT INTO public.device_session_nonces
                (session_id, nonce)
            VALUES (%s, %s)
            """,
            (session_id, nonce),
        )

        # The nonce is consumed only if the transaction commits.
        connection.commit()
        return plaintext

    except UniqueViolation as exc:
        if connection:
            connection.rollback()

        audit_replay(registered_device_id, session_id)
        raise ValueError("Replay detected") from exc

    except Exception:
        if connection:
            connection.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
