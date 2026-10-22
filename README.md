# IoMT Data Mesh Simulator (10 devices)

## What this repository contains

- **One simulator process** generates telemetry for all configured devices.
- `device_registry.json` holds development-time device configurations. All 10 sample devices route to the single logical mesh `MESH001`.
- Each telemetry event includes `data_mesh_id` and `registered_device_id`.
- One FastAPI WebSocket ingestion server validates the event shape and acknowledges receipt.
- Synthetic readings are demo data and must not be used for medical decisions.

This repository is a simulation/ingestion starter. It currently does **not** include your main application database, login/authentication service, durable telemetry storage, or patient dashboard API. The server validates device ID, device type, and mesh membership against the development registry, then logs and acknowledges messages. It does not persist them yet; PostgreSQL integration is the next step.

## Architecture

```text
One Python simulator process
  |-- D001 ... D010 (independent device configurations)
  |       each message: data_mesh_id + registered_device_id + type + timestamp + telemetry
  v
One WebSocket ingestion server (/ws)
  v
Validate device exists + device type + MESH001 membership
  v
[Next integration: durable storage / event broker]
  v
[Main backend: resolve registered_device_id -> current authorized user]
  v
Frontend dashboard (user's registered devices and latest readings)
```

`data_mesh_id` identifies the logical mesh/unit in this sample. `registered_device_id` identifies the registered device. Do not infer `user_id` from a device ID or trust a `user_id` supplied by a simulator. Resolve ownership in the authenticated backend/registration database, because a device may be reassigned and a user may own multiple devices.

## Run locally

Use Python 3.11+ (Python 3.12 recommended). Run commands from the `iotm-data-mesh` directory.

### 1. Create an environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
python -m pip install -r requirements.txt
```

### 2. Start the WebSocket server (terminal 1)

```bash
uvicorn data_mesh.websocket_server:app --host 127.0.0.1 --port 8000 --reload
```

Check `http://127.0.0.1:8000/health`.

### 3. Start the simulator (terminal 2)

```bash
python simulator/simulator.py
```

It loads all ten devices from `device_registry.json` and sends a telemetry event for each device approximately every five seconds. The ten events are sent sequentially over one WebSocket connection. This is approximately 2 messages/second on average (10 devices / 5 seconds), plus small processing/acknowledgement overhead.

## Configuration

- `IOMT_WS_URL`: WebSocket server URL. Default: `ws://127.0.0.1:8000/ws`.
- `IOMT_REGISTRY_PATH`: path to a JSON device registry. Default: `device_registry.json` in this repository.
- `LOG_LEVEL`: log level, default `INFO`.

For a remote server, set `IOMT_WS_URL` to its `wss://.../ws` endpoint. Do not expose an unauthenticated ingestion endpoint to the public internet. Add authentication and TLS before deployment.

## Device registry format

All 10 sample devices use `data_mesh_id: "MESH001"` because this project is configured for one Data Mesh receiving all device streams. Each entry has `data_mesh_id`, `registered_device_id`, `device_type`, and optional `telemetry_interval_seconds`. Device IDs must be unique in this sample registry. User IDs are deliberately not hardcoded. Replace the JSON registry with an authenticated lookup from your main backend/registration database when integrating dynamic registrations.

## Docker

Build and run the ingestion server:

```bash
docker build -t iomt-data-mesh .
docker run --rm -p 8000:8000 iomt-data-mesh
```

Run the simulator on the host in another terminal, or in another container on the same Docker network. If the simulator runs in a container, `127.0.0.1` refers to that simulator container itself; set `IOMT_WS_URL` to `ws://<server-service-name>:8000/ws` on a shared Docker network.

## Important integration steps before production

1. Authenticate the simulator/device gateway (API token, mTLS, or another service identity).
2. Replace the development JSON lookup with the authoritative registration database and verify each `registered_device_id` belongs to `data_mesh_id`.
3. Resolve the current user association server-side; do not accept an arbitrary user ID from telemetry.
4. Persist telemetry to your chosen database/time-series store or publish to a broker before acknowledging durable receipt.
5. Add message IDs/idempotency, rate limits, schema/versioning, retention rules, monitoring, and tests.
6. Validate clinically meaningful ranges and mark simulated data clearly. These random demo readings are not clinically validated.
