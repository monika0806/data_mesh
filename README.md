# IoMT Data Mesh Ingestion (Azure Blob Storage)

This project accepts synthetic IoMT telemetry over WebSocket, validates each event against the authoritative PostgreSQL registration table, then persists one JSON blob per event to a private Azure Blob Storage container.

> Demo only: synthetic measurements are not clinically validated and must not be used for medical decisions. Before production, add device/client authentication, authorization, rate limits, retention policy, monitoring, and privacy/security review.

## Architecture

```text
Simulator / device gateway
  -> WebSocket /ws
  -> validate event schema and supported device type
  -> PostgreSQL public.devices lookup by registered_device_id
  -> verify status is eligible and registered device_type matches
  -> assign logical product-1 ... product-5
  -> write JSON event to private Azure Blob Storage container
  -> acknowledge only after successful Blob upload
```

PostgreSQL is a registration lookup only. This service does not create or write a PostgreSQL telemetry table. The registered device ID in the event is always checked against `public.devices`; the local `device_registry.json` is for simulator samples only and is not authoritative.

## Product mapping

| Logical product ID | Product |
|---|---|
| `product-1` | Smartwatch readings |
| `product-2` | Blood pressure readings |
| `product-3` | Glucose readings |
| `product-4` | ECG readings |
| `product-5` | Pulse oximeter readings |
| Product 6 | Device identity/mapping concept; identity remains sourced from `public.devices` |

Product IDs are logical identifiers in this starter code, not a connection to an external Data Product catalog. Connect these IDs to your actual product registry when available.

## Required environment variables

Set these in Azure App Service > Settings > Environment variables (or Configuration, depending on portal UI):

- `IOMT_REGISTRATION_DATABASE_URL`: PostgreSQL connection URL for the database containing `public.devices`. It must be a least-privilege read-only account if possible.
- `AZURE_STORAGE_CONNECTION_STRING`: connection string for the storage account that owns the private container. Treat it as a secret.
- `IOMT_TELEMETRY_CONTAINER`: `iomt-telemetry` (default).
- `IOMT_ALLOWED_DEVICE_STATUSES`: comma-separated eligible statuses. Default: `active,registered,enabled`. **Confirm actual values in your database and change this setting to match.** Unknown/blank statuses are rejected.
- `LOG_LEVEL`: optional, default `INFO`.

The service accepts `IOMT_DATABASE_URL` as a temporary fallback for `IOMT_REGISTRATION_DATABASE_URL`; prefer the purpose-specific variable.

Never commit connection strings, passwords, storage keys, or real patient telemetry to source control. If a PostgreSQL password contains URL-reserved characters, URL-encode them in the connection URL.

## Device type mapping

The `public.devices.device_type` values must map to one of these types (case-insensitive, spaces/hyphens normalized): `smartwatch`, `smart_watch`, `smart watch`, `blood_pressure`, `blood_pressure_monitor`, `bp_monitor`, `blood pressure monitor`, `glucose`, `glucose_monitor`, `glucose monitor`, `ecg`, `ecg_monitor`, `ecg monitor`, `pulse_oximeter`, `pulse oximeter`.

If your database uses other values, update `PRODUCT_TYPE_ALIASES` in `data_mesh/websocket_server.py` after checking the real values. The service rejects unsupported or mismatched types.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export IOMT_REGISTRATION_DATABASE_URL='postgresql://...'
export AZURE_STORAGE_CONNECTION_STRING='...'
export IOMT_TELEMETRY_CONTAINER='iomt-telemetry'
uvicorn data_mesh.websocket_server:app --host 127.0.0.1 --port 8000
```

Do not paste secrets into chat or commit them. `/health` checks both dependencies once the variables are configured. `/ws` is a WebSocket endpoint; visiting it as a normal browser HTTP URL can return 404 and that alone does not indicate a failure.

## Azure deployment checklist

1. Configure the environment variables above in App Service. Prefer managed identity + RBAC instead of account keys when adapting the code for production.
2. Ensure App Service can reach PostgreSQL on port 5432 and has the required firewall/network permissions.
3. Ensure the storage connection targets the account containing the `iomt-telemetry` private container.
4. Restart/redeploy the app after changing settings.
5. Open `/health` and verify `registration_database: connected` and `blob_storage: connected`.
6. Send a test event using a real registered device ID and the exact device type/status from PostgreSQL.
7. Confirm a JSON blob appears under `product-name/registered_device_id/YYYY/MM/DD/...json`.
8. Test unknown ID, ineligible status, mismatched type, database outage, and storage outage; none should be acknowledged as stored.

## Important limitations

- This code is not yet connected to an external Data Product catalog; product IDs are logical.
- Default eligible status values must be confirmed against the real `public.devices` table.
- The service currently uses synchronous database and Blob SDK calls in the WebSocket handler; consider threadpool/async clients for higher throughput.
- Authenticate devices before exposing the WebSocket endpoint publicly.
- The sample simulator contains fake IDs such as D001–D010. They will be rejected unless they actually exist in `public.devices` with matching status/type.
- Product 6 is described as a logical identity product here; the actual product registry/catalog integration still needs implementation.
