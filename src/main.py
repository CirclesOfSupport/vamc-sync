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

# Fields that constitute a change — sta_num is the key, everything else is compared
COMPARE_FIELDS = ["main_vamc", "state", "zip_start", "common_name", "short_code", "city", "display_name"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/bigquery",
]


def is_root_sta(sta):
    """Root-level Sta# is numeric only — no alpha suffix (e.g. '605', not '605A4')."""
    return bool(re.match(r'^\d+$', sta.strip()))


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def get_credentials():
    """Get refreshed OAuth credentials covering both Sheets and BigQuery scopes."""
    creds = Credentials(
        token=None,
        refresh_token=os.environ.get("OAUTH_REFRESH_TOKEN"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ.get("OAUTH_CLIENT_ID"),
        client_secret=os.environ.get("OAUTH_CLIENT_SECRET"),
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds


def get_sheets_client():
    """Build Sheets API client using OAuth credentials."""
    return build("sheets", "v4", credentials=get_credentials())


def get_bq_client():
    """Build BigQuery client using same OAuth credentials as Sheets."""
    return bigquery.Client(project=BQ_PROJECT, credentials=get_credentials())


# ---------------------------------------------------------------------------
# Sheet reading
# ---------------------------------------------------------------------------

def read_sheet_rows():
    """
    Read all root-level Sta# rows from Sheet1.
    Root-level = numeric Sta# only (e.g. '605', not '605A4').
    Covers all 129 parent VA facilities regardless of Type classification.
    Returns dict keyed by sta_num.
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

    try:
        sta_idx = headers.index("Sta#")
    except ValueError:
        raise ValueError(f"Column 'Sta#' not found in sheet headers: {headers}")

    col_indices = {}
    for sheet_col, bq_field in COLUMN_MAP:
        try:
            col_indices[bq_field] = headers.index(sheet_col)
        except ValueError:
            raise ValueError(f"Column '{sheet_col}' not found in sheet headers: {headers}")

    rows = {}
    for row in all_rows[1:]:
        sta = row[sta_idx].strip() if sta_idx < len(row) else ""
        if not sta or not is_root_sta(sta):
            continue
        record = {}
        for bq_field, idx in col_indices.items():
            record[bq_field] = row[idx].strip() if idx < len(row) else ""
        rows[sta] = record

    logger.info(f"Read {len(rows)} root-level rows from sheet")
    return rows


# ---------------------------------------------------------------------------
# BQ helpers
# ---------------------------------------------------------------------------

def get_bq_rows(client):
    """
    Read current vamc_reference contents from BQ.
    Returns dict keyed by sta_num.
    """
    table_ref = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    query = f"SELECT * FROM {table_ref}"
    try:
        result = client.query(query).result()
        rows = {}
        for row in result:
            d = dict(row)
            rows[d["sta_num"]] = d
        logger.info(f"Read {len(rows)} rows from BQ")
        return rows
    except Exception:
        # Table may not exist yet on first run
        logger.info("BQ table not found or empty — treating as empty")
        return {}


def ensure_table(client):
    """Create vamc_reference table if it doesn't exist."""
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


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------

def compute_delta(sheet_rows, bq_rows):
    """
    Compare sheet rows against BQ rows and return inserts, updates, deletes.
    """
    to_insert = []
    to_update = []
    to_delete = []

    for sta, sheet_row in sheet_rows.items():
        if sta not in bq_rows:
            to_insert.append(sheet_row)
        else:
            bq_row = bq_rows[sta]
            changed = any(
                sheet_row.get(f, "") != str(bq_row.get(f, "") or "")
                for f in COMPARE_FIELDS
            )
            if changed:
                to_update.append(sheet_row)

    for sta in bq_rows:
        if sta not in sheet_rows:
            to_delete.append(sta)

    return to_insert, to_update, to_delete


def apply_delta(client, to_insert, to_update, to_delete):
    """Apply inserts, updates, and deletes to vamc_reference."""
    table_ref = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    full_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    if to_insert:
        errors = client.insert_rows_json(full_ref, to_insert)
        if errors:
            raise RuntimeError(f"BQ insert errors: {errors}")
        logger.info(f"Inserted {len(to_insert)} rows")

    for row in to_update:
        sta = row["sta_num"]
        set_clauses = ", ".join(
            f"{f} = @{f}" for f in COMPARE_FIELDS
        )
        params = [
            bigquery.ScalarQueryParameter(f, "STRING", row.get(f, ""))
            for f in COMPARE_FIELDS
        ]
        params.append(bigquery.ScalarQueryParameter("sta_num", "STRING", sta))
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        client.query(
            f"UPDATE {table_ref} SET {set_clauses} WHERE sta_num = @sta_num",
            job_config=job_config
        ).result()
    if to_update:
        logger.info(f"Updated {len(to_update)} rows")

    if to_delete:
        sta_list = ", ".join(f"'{s}'" for s in to_delete)
        client.query(
            f"DELETE FROM {table_ref} WHERE sta_num IN ({sta_list})"
        ).result()
        logger.info(f"Deleted {len(to_delete)} rows")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/sync", methods=["POST"])
def sync():
    """
    Sync VA facility reference data from Google Sheet to BigQuery.

    Compares sheet against current BQ state and applies only the delta —
    inserts for new rows, updates for changed rows, deletes for removed rows.
    If the sheet read fails, BQ is untouched.

    Called nightly by Cloud Scheduler or on-demand after sheet edits.

    Request body (JSON):
    {
        "password": "<SYNC_PASSWORD>"
    }

    Response:
    {
        "status": "success",
        "inserted": 0,
        "updated": 2,
        "deleted": 0
    }
    """
    body = request.get_json(force=True, silent=True) or {}

    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    try:
        # Read sheet first — if this fails, BQ is never touched
        sheet_rows = read_sheet_rows()

        client = get_bq_client()
        ensure_table(client)
        bq_rows = get_bq_rows(client)

        to_insert, to_update, to_delete = compute_delta(sheet_rows, bq_rows)

        if not to_insert and not to_update and not to_delete:
            logger.info("No changes detected")
            return jsonify({"status": "success", "inserted": 0, "updated": 0, "deleted": 0}), 200

        apply_delta(client, to_insert, to_update, to_delete)

        return jsonify({
            "status": "success",
            "inserted": len(to_insert),
            "updated": len(to_update),
            "deleted": len(to_delete),
        }), 200

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
