# vamc-sync

Cloud Run service that syncs VAMC reference data from the VA Zip Codes by Facility Google Sheet to BigQuery.

## What it does

- Reads all root-level Sta# rows (129) from Sheet1 of the VA Zip Codes by Facility sheet
- Root-level = numeric Sta# only (e.g. `605`, not `605A4`) — covers all 129 parent VA facilities regardless of Type classification
- Compares sheet data against the current state of `early-alert-responses.RESPONSES.vamc_reference` in BigQuery and applies only the delta — inserts for new rows, updates for changed rows, deletes for removed rows
- If the sheet read fails for any reason, BigQuery is untouched
- Called nightly by Cloud Scheduler and on-demand after sheet edits

## BQ table: `vamc_reference`

| Field | Type | Source |
|---|---|---|
| sta_num | STRING | Sta# column — primary key; also the value stored in `vamc_presumed` on subscriber records |
| main_vamc | STRING | Main VAMC column (all-caps, as-is) |
| state | STRING | State column |
| zip_start | STRING | Zip Start column (STRING to preserve leading zeros) |
| common_name | STRING | VA Facility Common Name column (user-edited) |
| short_code | STRING | ShortCode column (user-edited) |
| city | STRING | City column (user-edited) |
| display_name | STRING | Display Name column — formula-driven in sheet; format: `State: [ShortCode] Common Name (City)` |

**Looker join:** `users.vamc_presumed` (contains Sta#) → `vamc_reference.sta_num` → `vamc_reference.display_name`

## Environment variables

| Variable | Description |
|---|---|
| OAUTH_CLIENT_ID | OAuth 2.0 client ID (same as sheet-service) |
| OAUTH_CLIENT_SECRET | OAuth 2.0 client secret (same as sheet-service) |
| OAUTH_REFRESH_TOKEN | OAuth refresh token (same as sheet-service) |
| SYNC_PASSWORD | Password required in POST body to trigger sync |
| GCP_PROJECT | GCP project ID (default: early-alert-responses) |

## Endpoints

- `GET /health` — health check
- `POST /sync` — trigger sync; body: `{"password": "<SYNC_PASSWORD>"}`

Response:
```json
{"status": "success", "inserted": 0, "updated": 2, "deleted": 0}
```

## Deployment

Deployed via Cloud Build trigger on push to main branch in CirclesOfSupport/vamc-sync.
Uses `webhook-repo` Artifact Registry in us-east1.

The service uses `--no-allow-unauthenticated` — only Cloud Scheduler (via OIDC) and
authorized callers can invoke it.

## Cloud Scheduler

Nightly trigger at 2:00 AM ET:
- Schedule: `0 7 * * *` (UTC)
- Target: POST `https://<service-url>/sync`
- Body: `{"password": "<SYNC_PASSWORD>"}`
- Auth: OIDC token with the Cloud Run invoker service account
