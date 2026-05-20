import os
import re
import logging
from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.cloud import bigquery

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHEET_ID = "11WPuREMweXudRt2kHU-qcYOGjClSY8Y8hE4ku4voGM8"
SHEET_TAB = "Sheet1"

BQ_PROJECT = os.environ.get("GCP_PROJECT", "early-alert-responses")
BQ_DATASET = "RESPONSES"
BQ_TABLE = "vamc_reference"

SYNC_PASSWORD = os.environ.get("SYNC_PASSWORD", "")

# Columns to read from sheet → BQ field names
# Format: (sheet_header, bq_field)
COLUMN_MAP = [
    ("Sta#",                    "sta_num"),
    ("Main VAMC",               "main_vamc"),
    ("State",                   "state"),
    ("Zip Start",               "zip_start"),
    ("VA Facility Common Name", "common_name"),
    ("ShortCode",               "short_code"),
    ("City",                    "city"),
    ("Display Name",            "display_name"),
]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def is_root_sta(sta):
    """Root-level Sta# is numeric only — no alpha suffix (e.g. '605', not '605A4')."""
    return bool(re.match(r'^\d+$', sta.strip()))


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def get_sheets_client():
    """Build Sheets API client using OAuth refresh token (same pattern as sheet-service)."""
    creds = Credentials(
        token=None,
        refresh_token=os.environ.get("OAUTH_REFRESH_TOKEN"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ.get("OAUTH_CLIENT_ID"),
        client_secret=os.environ.get("OAUTH_CLIENT_SECRET"),
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)


def get_bq_client():
    """Build BigQuery client using ambient credentials (Cloud Run service account)."""
    return bigquery.Client(project=BQ_PROJECT)


# ---------------------------------------------------------------------------
# Sheet reading
# ---------------------------------------------------------------------------

def read_vamc_rows():
    """
    Read all root-level Sta# rows from Sheet1.
    Root-level = numeric Sta# only (no alpha suffix).
    This gives 129 rows covering all parent VA facilities regardless of Type.
    Returns list of dicts with BQ field names as keys.
    """
    service = get_sheets_client()
    spreadsheet = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{SHEET_TAB}'!A1:ZZ",
        valueRenderOption="FORMATTED_VALUE",
    ).execute()

    all_rows = spreadsheet.get("values", [])
    if not all_rows:
        raise ValueError("Sheet returned no data")

    headers = [h.strip() for h in all_rows[0]]

    # Locate Sta# column index
    try:
        sta_idx = headers.index("Sta#")
    except ValueError:
        raise ValueError(f"Column 'Sta#' not found in sheet headers: {headers}")

    # Locate all mapped column indices
    col_indices = {}
    for sheet_col, bq_field in COLUMN_MAP:
        try:
            col_indices[bq_field] = headers.index(sheet_col)
        except ValueError:
            raise ValueError(f"Column '{sheet_col}' not found in sheet headers: {headers}")

    vamc_rows = []
    for row in all_rows[1:]:
        sta = row[sta_idx].strip() if sta_idx < len(row) else ""
        if not sta or not is_root_sta(sta):
            continue

        record = {}
        for bq_field, idx in col_indices.items():
            record[bq_field] = row[idx].strip() if idx < len(row) else ""

        vamc_rows.append(record)

    logger.info(f"Read {len(vamc_rows)} root-level VA facility rows from sheet")
    return vamc_rows


# ---------------------------------------------------------------------------
# BQ sync
# ---------------------------------------------------------------------------

def sync_to_bq(rows):
    """
    Replace vamc_reference table contents with the provided rows.
    Uses TRUNCATE + INSERT pattern for simplicity given the small table size (129 rows).
    """
    client = get_bq_client()
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    schema = [
        bigquery.SchemaField("sta_num",      "STRING"),
        bigquery.SchemaField("main_vamc",    "STRING"),
        bigquery.SchemaField("state",        "STRING"),
        bigquery.SchemaField("zip_start",    "STRING"),
        bigquery.SchemaField("common_name",  "STRING"),
        bigquery.SchemaField("short_code",   "STRING"),
        bigquery.SchemaField("city",         "STRING"),
        bigquery.SchemaField("display_name", "STRING"),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)

    # Truncate existing rows
    client.query(f"TRUNCATE TABLE `{table_ref}`").result()
    logger.info(f"Truncated {table_ref}")

    # Insert new rows
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors}")

    logger.info(f"Inserted {len(rows)} rows into {table_ref}")
    return len(rows)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/sync", methods=["POST"])
def sync():
    """
    Trigger a sync from Google Sheet to BigQuery.

    Called by Cloud Scheduler nightly, or manually for the initial load.

    Request body (JSON):
    {
        "password": "<SYNC_PASSWORD>"
    }

    Response:
    {
        "status": "success",
        "rows_synced": 129
    }
    """
    body = request.get_json(force=True, silent=True) or {}

    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    try:
        rows = read_vamc_rows()
        count = sync_to_bq(rows)
        return jsonify({"status": "success", "rows_synced": count}), 200

    except ValueError as e:
        logger.exception("Data error during sync")
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        logger.exception("Unexpected error during sync")
        return jsonify({"status": "error", "message": str(e)}), 500


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
