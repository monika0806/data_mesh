import base64
import uuid

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from psycopg2.errors import UniqueViolation

from data_mesh import secure_session as secure


class FakeCursor:
    def __init__(self, row, replay=False):
        self.row = row
        self.replay = replay
        self.calls = 0

    def execute(self, query, params=None):
        self.calls += 1

        if self.calls == 2 and self.replay:
            raise UniqueViolation("duplicate nonce")

    def fetchone(self):
        return self.row

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def make_test_session(monkeypatch, replay=False, status="active"):
    master_key = AESGCM.generate_key(bit_length=256)
    session_key = AESGCM.generate_key(bit_length=256)

    monkeypatch.setenv(
        "IOMT_SESSION_ENCRYPTION_KEY",
        base64.b64encode(master_key).decode("ascii"),
    )

    session_id = str(uuid.uuid4())
    device_record_id = 23
    registered_device_id = "IOMT-TEST-DEVICE"

    storage_nonce = AESGCM.generate_key(bit_length=128)[:12]
    encrypted_session_key = AESGCM(master_key).encrypt(
        storage_nonce,
        session_key,
        secure.session_aad(session_id, device_record_id),
    )

    payload = b'{"heart_rate":72,"test":true}'
    payload_nonce = AESGCM.generate_key(bit_length=128)[:12]
    ciphertext = AESGCM(session_key).encrypt(
        payload_nonce,
        payload,
        session_id.encode("ascii"),
    )

    row = (
        device_record_id,
        status,
        encrypted_session_key,
        storage_nonce,
    )

    cursor = FakeCursor(row, replay=replay)
    connection = FakeConnection(cursor)

    monkeypatch.setattr(
        secure,
        "get_db_connection",
        lambda: connection,
    )

    return (
        connection,
        session_id,
        registered_device_id,
        payload_nonce,
        ciphertext,
        payload,
    )


def test_master_key_accepts_valid_32_byte_key(monkeypatch):
    key = AESGCM.generate_key(bit_length=256)
    monkeypatch.setenv(
        "IOMT_SESSION_ENCRYPTION_KEY",
        base64.b64encode(key).decode("ascii"),
    )

    assert secure.get_master_key() == key


def test_master_key_rejects_invalid_length(monkeypatch):
    monkeypatch.setenv(
        "IOMT_SESSION_ENCRYPTION_KEY",
        base64.b64encode(b"too-short").decode("ascii"),
    )

    with pytest.raises(RuntimeError, match="must be 32 bytes"):
        secure.get_master_key()


def test_session_aad_is_bound_to_session_and_device():
    session_id = str(uuid.uuid4())

    aad = secure.session_aad(session_id, 23)

    assert aad == (
        b"iomt-session-store-v1:"
        + session_id.encode("ascii")
        + b":23"
    )


def test_decrypt_and_consume_accepts_valid_payload(monkeypatch):
    (
        connection,
        session_id,
        device_id,
        nonce,
        ciphertext,
        expected_payload,
    ) = make_test_session(monkeypatch)

    plaintext = secure.decrypt_and_consume(
        session_id,
        device_id,
        nonce,
        ciphertext,
    )

    assert plaintext == expected_payload
    assert connection.committed
    assert not connection.rolled_back
    assert connection.closed


def test_decrypt_and_consume_rejects_inactive_device(monkeypatch):
    (
        connection,
        session_id,
        device_id,
        nonce,
        ciphertext,
        _,
    ) = make_test_session(monkeypatch, status="revoked")

    with pytest.raises(ValueError, match="Device is not permitted"):
        secure.decrypt_and_consume(
            session_id,
            device_id,
            nonce,
            ciphertext,
        )

    assert connection.rolled_back
    assert not connection.committed


def test_decrypt_and_consume_rejects_replayed_nonce(monkeypatch):
    (
        connection,
        session_id,
        device_id,
        nonce,
        ciphertext,
        _,
    ) = make_test_session(monkeypatch, replay=True)

    audit_events = []
    monkeypatch.setattr(
        secure,
        "audit_replay",
        lambda device, session: audit_events.append((device, session)),
    )

    with pytest.raises(ValueError, match="Replay detected"):
        secure.decrypt_and_consume(
            session_id,
            device_id,
            nonce,
            ciphertext,
        )

    assert connection.rolled_back
    assert not connection.committed
    assert audit_events == [(device_id, session_id)]


def test_decrypt_and_consume_rolls_back_invalid_ciphertext(monkeypatch):
    (
        connection,
        session_id,
        device_id,
        nonce,
        ciphertext,
        _,
    ) = make_test_session(monkeypatch)

    tampered_ciphertext = ciphertext[:-1] + bytes([ciphertext[-1] ^ 1])

    with pytest.raises(Exception):
        secure.decrypt_and_consume(
            session_id,
            device_id,
            nonce,
            tampered_ciphertext,
        )

    assert connection.rolled_back
    assert not connection.committed
