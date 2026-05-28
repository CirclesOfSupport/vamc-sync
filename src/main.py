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

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------------------------------------------------------------------
# Join mode: controls how vamc_display_name is populated on users/response_data
#
# "name_string" (current): vamc_presumed contains mixed-case name strings.
#     JOIN condition: UPPER(vamc_presumed) = vamc_reference.main_vamc
#
# "sta_num" (after full Sta# backfill): vamc_presumed contains Sta# values.
#     JOIN condition: vamc_presumed = vamc_reference.sta_num
#
# Switch this env var to "sta_num" after the full BQ vamc_presumed backfill
# completes. No code change required.
# ---------------------------------------------------------------------------
JOIN_MODE = os.environ.get("VAMC_JOIN_MODE", "name_string")


def is_root_sta(sta):
    """Root-level Sta# is numeric only — no alpha suffix (e.g. '605', not '605A4')."""
    return bool(re.match(r'^\d+$', sta.strip()))


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def get_sheets_client():
    """Build Sheets API client using OAuth refresh token."""
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
    """Build BigQuery client using ambient service account credentials (ADC)."""
    return bigquery.Client(project=BQ_PROJECT)


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
# Display name population
# ---------------------------------------------------------------------------

def populate_display_names(client):
    """
    Populate vamc_display_name on users and response_data for any rows
    where the stored vamc_display_name is missing OR doesn't match the
    current vamc_reference.display_name for the joined Sta#.

    JOIN mode is controlled by VAMC_JOIN_MODE env var:
      "name_string" (default): JOIN on UPPER(vamc_presumed) = vamc_reference.main_vamc
      "sta_num": JOIN on vamc_presumed = vamc_reference.sta_num

    Touches rows where vamc_display_name IS NULL, is empty, or is stale
    (differs from the joined r.display_name). Safe to run repeatedly —
    once a row's display_name matches the reference, it stops matching
    the WHERE clause.
    """
    ref = f"`{BQ_PROJECT}.{BQ_DATASET}.vamc_reference`"

    if JOIN_MODE == "sta_num":
        join_condition = "t.vamc_presumed = r.sta_num"
        logger.info("populate_display_names: using sta_num join mode")
    else:
        join_condition = "UPPER(t.vamc_presumed) = r.main_vamc"
        logger.info("populate_display_names: using name_string join mode")

    results = {}

    for target_table in ["users", "response_data"]:
        tbl = f"`{BQ_PROJECT}.{BQ_DATASET}.{target_table}`"

        # For response_data, restrict to rows before today to avoid concurrent
        # write conflicts from whatever processes are actively writing to the
        # table. Today's rows are picked up on the next nightly run.
        date_filter = (
            "AND t.checkinDateTime < DATETIME(CURRENT_DATE())"
            if target_table == "response_data"
            else ""
        )

        query = f"""
            UPDATE {tbl} t
            SET t.vamc_display_name = r.display_name
            FROM {ref} r
            WHERE {join_condition}
              AND t.vamc_presumed IS NOT NULL
              AND t.vamc_presumed != ''
              AND (
                t.vamc_display_name IS NULL
                OR t.vamc_display_name = ''
                OR t.vamc_display_name != r.display_name
              )
              {date_filter}
        """
        try:
            job = client.query(query)
            job.result()
            rows_affected = job.num_dml_affected_rows or 0
            logger.info(f"populate_display_names: {target_table} — {rows_affected} rows updated")
            results[target_table] = rows_affected
        except Exception as e:
            # Concurrent write conflict on response_data is expected during high traffic.
            # Log and continue — next scheduled run will catch remaining rows.
            logger.warning(f"populate_display_names: {target_table} failed — {e}")
            results[target_table] = f"error: {str(e)}"

    return results


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/sync", methods=["POST"])
def sync():
    """
    Sync VA facility reference data from Google Sheet to BigQuery,
    then populate vamc_display_name on users and response_data.

    Phase 1: Compare sheet against vamc_reference and apply delta
             (inserts/updates/deletes). If sheet read fails, vamc_reference
             is untouched but Phase 2 still runs.

    Phase 2: UPDATE vamc_display_name on users and response_data for any
             rows with vamc_presumed set but vamc_display_name not yet
             populated. Runs regardless of Phase 1 outcome — the two phases
             are independent. JOIN mode controlled by VAMC_JOIN_MODE env var.
             Concurrent write conflicts on response_data are logged and
             tolerated — next run will catch remaining rows.

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
        "deleted": 0,
        "display_names": {
            "users": 142,
            "response_data": 1834
        }
    }
    """
    body = request.get_json(force=True, silent=True) or {}

    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    client = get_bq_client()
    phase1_result = {}
    phase1_error = None

    # Phase 1: Sheet → vamc_reference sync
    # Runs independently of Phase 2. If sheet read fails, vamc_reference is
    # untouched but Phase 2 still runs against the existing reference data.
    try:
        sheet_rows = read_sheet_rows()
        ensure_table(client)
        bq_rows = get_bq_rows(client)
        to_insert, to_update, to_delete = compute_delta(sheet_rows, bq_rows)

        if to_insert or to_update or to_delete:
            apply_delta(client, to_insert, to_update, to_delete)
        else:
            logger.info("Phase 1: No vamc_reference changes detected")

        phase1_result = {
            "inserted": len(to_insert),
            "updated": len(to_update),
            "deleted": len(to_delete),
        }
    except Exception as e:
        logger.exception("Phase 1 (sheet sync) failed — proceeding to Phase 2")
        phase1_error = str(e)
        phase1_result = {"inserted": 0, "updated": 0, "deleted": 0}

    # Phase 2: Populate vamc_display_name on users and response_data
    # Runs regardless of Phase 1 outcome. Reads from vamc_reference as-is.
    try:
        display_name_results = populate_display_names(client)
    except Exception as e:
        logger.exception("Phase 2 (display name population) failed")
        display_name_results = {"error": str(e)}

    response = {
        "status": "success" if not phase1_error else "partial",
        **phase1_result,
        "display_names": display_name_results,
    }
    if phase1_error:
        response["phase1_error"] = phase1_error

    return jsonify(response), 200


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
