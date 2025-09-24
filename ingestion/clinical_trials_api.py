import os
import json
import time
import logging
from datetime import datetime
from math import ceil
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path

import requests
import snowflake.connector
from dotenv import load_dotenv

# Load .env if present (handy for local dev; no secrets in repo)
load_dotenv()


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("ctgov")


BASE_URL     = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE    = int(os.getenv("CTGOV_PAGE_SIZE", "100"))      # 1..1000
MAX_PAGES    = int(os.getenv("CTGOV_MAX_PAGES", "5"))        # 0 = all pages
MAX_RECORDS  = int(os.getenv("CTGOV_MAX_RECORDS", "0"))      # 0 = unlimited
START_DATE   = os.getenv("CTGOV_START_DATE", "2015-01-01")
END_DATE     = os.getenv("CTGOV_END_DATE", "MAX")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
SINK         = os.getenv("SINK", "snowflake").lower()        # "file" | "snowflake"
INSERT_CHUNK = int(os.getenv("SNOWFLAKE_INSERT_CHUNK", "500"))

# Spec filter: Interventional + Phase 2/3 + StudyFirstSubmitDate ≥ START_DATE
ADVANCED_ESSIE = (
    f"(AREA[StudyType]INTERVENTIONAL) "
    f"AND (AREA[Phase](PHASE2 OR PHASE3)) "
    f"AND AREA[StudyFirstSubmitDate]RANGE[{START_DATE},{END_DATE}]"
)

# Limit fields returned (lighter payload)
FIELDS = [
    "NCTId",
    "BriefTitle",
    "OverallStatus",
    "StudyType",
    "Phase",
    "StudyFirstSubmitDate",
    "HasResults",
    "ProtocolSection",
    "StatusModule",
]
FIELDS_PARAM = ",".join(FIELDS)

COMMON_PARAMS: Dict[str, str] = {
    "pageSize": str(PAGE_SIZE),
    "fields": FIELDS_PARAM,
    "countTotal": "true",
    "sort": "LastUpdatePostDate:desc",
    "filter.advanced": ADVANCED_ESSIE,
}

# ---------------- HTTP -------------------
def _get(params: Dict[str, str]) -> Optional[dict]:
    """Perform a GET with simple retries and return JSON."""
    for attempt in range(1, 4):
        try:
            r = requests.get(BASE_URL, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code >= 400:
                snippet = r.text[:500].replace("\n", " ")
                log.error("HTTP %s (attempt %d): %s", r.status_code, attempt, snippet)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning("GET attempt %d failed: %s", attempt, e)
            time.sleep(attempt)  # backoff: 1s, 2s
    return None

def get_api_data() -> List[dict]:
    """Fetch all pages (bounded by MAX_PAGES/MAX_RECORDS)."""
    studies: List[dict] = []
    page_token: Optional[str] = None
    seen_tokens: Set[str] = set()
    pages = 0
    expected_pages: Optional[int] = None

    while True:
        params = dict(COMMON_PARAMS)
        if page_token:
            params["pageToken"] = page_token

        payload = _get(params)
        if payload is None:
            break

        if expected_pages is None and isinstance(payload.get("totalCount"), int):
            expected_pages = max(1, ceil(payload["totalCount"] / PAGE_SIZE))
            log.info("totalCount=%s (~%s pages)", payload["totalCount"], expected_pages)

        batch = payload.get("studies", []) or []
        studies.extend(batch)
        pages += 1
        log.info(
            "Fetched page %d: %d rows (total=%d)%s",
            pages,
            len(batch),
            len(studies),
            ", nextPageToken" if "nextPageToken" in payload else "",
        )

        if MAX_RECORDS and len(studies) >= MAX_RECORDS:
            studies = studies[:MAX_RECORDS]
            log.info("Reached MAX_RECORDS=%d", MAX_RECORDS)
            break
        if MAX_PAGES and pages >= MAX_PAGES:
            log.info("Reached MAX_PAGES=%d", MAX_PAGES)
            break

        next_tok = payload.get("nextPageToken")
        if not next_tok:
            break
        if next_tok in seen_tokens:
            log.warning("nextPageToken repeated; stopping.")
            break
        seen_tokens.add(next_tok)
        page_token = next_tok

    log.info("collected %d studies", len(studies))
    return studies

# -------------- Secrets / Snowflake I/O ------------
def _env(name: str, default: Optional[str] = None) -> str:
    """Get required env var or raise."""
    v = os.getenv(name, default)
    if v is None or not str(v).strip():
        raise RuntimeError(f"Missing env var {name}")
    return str(v).strip()

def _load_snowflake_creds():
    """
    Load Snowflake creds from AWS Secrets Manager if SNOWFLAKE_SECRET_ARN is set.
    Secret JSON shape: { "account": "...", "user": "...", "password": "..." }
    Falls back to env vars otherwise.
    """
    secret_arn = os.getenv("SNOWFLAKE_SECRET_ARN")
    if secret_arn:
        try:
            import boto3  # available in Lambda base images; add to requirements for other runtimes
            sm = boto3.client("secretsmanager")
            resp = sm.get_secret_value(SecretId=secret_arn)
            payload = resp.get("SecretString") or resp.get("SecretBinary")
            raw = payload if isinstance(payload, str) else payload.decode()
            data = json.loads(raw)
            return data["account"], data["user"], data["password"]
        except Exception as e:
            log.error("Failed to load Snowflake creds from Secrets Manager: %s", e)
            # continue to env var fallback

    return (
        _env("SNOWFLAKE_ACCOUNT"),
        _env("SNOWFLAKE_USER"),
        _env("SNOWFLAKE_PASSWORD"),
    )

def _connect_snowflake():
    account, user, password = _load_snowflake_creds()
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CLINICAL_TRIALS_DEV"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.getenv("SNOWFLAKE_ROLE") or None,
    )

def _ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS RAW_CTGOV_STUDIES (
            RAW_DATA VARIANT,
            INGESTION_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID STRING
        )
        """
    )

def _chunks(rows: List[Tuple[str, str]], n: int):
    for i in range(0, len(rows), n):
        yield rows[i : i + n]

def save_to_snowflake(studies: List[dict]) -> None:
    if not studies:
        log.warning("No rows to save.")
        return

    conn = cur = None
    try:
        conn = _connect_snowflake()
        cur = conn.cursor()
        _ensure_table(cur)

        batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        rows = [(json.dumps(s), batch_id) for s in studies]

        # Single-row inserts inside chunks: avoids "Failed to rewrite multi-row insert"
        sql = """
            INSERT INTO RAW_CTGOV_STUDIES (RAW_DATA, INGESTION_TIMESTAMP, BATCH_ID)
            SELECT PARSE_JSON(%s), CURRENT_TIMESTAMP(), %s
        """

        inserted = 0
        for chunk in _chunks(rows, INSERT_CHUNK):
            for r in chunk:
                cur.execute(sql, r)
            conn.commit()
            inserted += len(chunk)
            log.info("Inserted %d/%d...", inserted, len(rows))

        log.info("✅ saved %d rows to RAW_CTGOV_STUDIES (batch_id=%s)", inserted, batch_id)

    except Exception as e:
        log.error("Snowflake save failed: %s", e, exc_info=True)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass

# -------------- File I/O (local demo) ----------
def save_to_file(studies: List[dict]) -> None:
    out = Path("artifacts")
    out.mkdir(parents=True, exist_ok=True)
    p = out / "raw_ctgov_studies.ndjson"
    with p.open("w", encoding="utf-8") as f:
        for s in studies:
            f.write(json.dumps(s) + "\n")
    log.info("💾 wrote %d rows to %s", len(studies), p)

# ---------------- Entrypoints ------------
def main():
    log.info(
        "Start ingestion | sink=%s | pageSize=%s | maxPages=%s",
        SINK,
        PAGE_SIZE,
        MAX_PAGES,
    )
    studies = get_api_data()
    if not studies:
        log.warning("No studies returned.")
        return

    if SINK == "file":
        save_to_file(studies)
    else:
        save_to_snowflake(studies)

def lambda_handler(event, context):
    main()
    return {"statusCode": 200, "body": "OK"}

if __name__ == "__main__":
    main()
