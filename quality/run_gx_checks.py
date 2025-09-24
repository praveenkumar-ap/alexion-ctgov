#!/usr/bin/env python3
"""
Great Expectations checks for our Snowflake models.

- Checks STAGING.STG_CTGOV_STUDIES: NCT_ID is not null
- Checks MARTS.FCT_CLINICAL_TRIAL_EARLY_STOPS: EARLY_STOP_RATE in [0, 1]

Exits non-zero if any expectation fails.
"""

import os
import sys
import logging
import traceback

import pandas as pd
import snowflake.connector
import great_expectations as gx


# -------- logging --------
LOG_LEVEL = os.getenv("GX_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s gx :: %(message)s",
)
log = logging.getLogger("gx")


# -------- helpers --------
def env(name: str, default: str | None = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing env var {name}")
    return str(v) if v is not None else ""


def connect_snowflake():
    # These come from your GitHub Actions secrets
    return snowflake.connector.connect(
        account=env("SNOWFLAKE_ACCOUNT", required=True),
        user=env("SNOWFLAKE_USER", required=True),
        password=env("SNOWFLAKE_PASSWORD", required=True),
        warehouse=env("SNOWFLAKE_WAREHOUSE", required=True),
        database=env("SNOWFLAKE_DATABASE", required=True),
        role=os.getenv("SNOWFLAKE_ROLE") or None,
        autocommit=True,
    )


def read_df(cur, sql: str) -> pd.DataFrame:
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame.from_records(rows, columns=cols)



def summarize_result(tag: str, result) -> bool:
    """Print a compact summary and return True if success, else False."""
    try:
        success = bool(result.success)
        # result.results may not always exist / be a list across versions; guard it
        passed = 0
        total = 0
        if hasattr(result, "results") and isinstance(result.results, list):
            total = len(result.results)
            passed = sum(1 for r in result.results if getattr(r, "success", False))
        log.info("%s -> success=%s, %d/%d expectations passed", tag, success, passed, total)
        # Print failures (if any)
        if not success and hasattr(result, "results"):
            for r in result.results or []:
                if not getattr(r, "success", False):
                    etype = getattr(getattr(r, "expectation_config", None), "expectation_type", "unknown")
                    log.error("%s failed: %s | info=%s", tag, etype, getattr(r, "result", {}))
        return success
    except Exception:
        log.error("%s -> could not summarize GE result:\n%s", tag, traceback.format_exc())
        return False


# -------- main --------
def main() -> int:
    try:
        conn = connect_snowflake()
        with conn.cursor() as cur:
            # Pull the two tables we validate (small, so pandas is fine for CI)
            df_stg = read_df(cur, "SELECT NCT_ID FROM STAGING.STG_CTGOV_STUDIES")
            df_fact = read_df(cur, "SELECT EARLY_STOP_RATE FROM MARTS.FCT_CLINICAL_TRIAL_EARLY_STOPS")

        if df_stg.empty:
            log.error("STAGING.STG_CTGOV_STUDIES returned 0 rows; cannot validate.")
            return 2
        if df_fact.empty:
            log.error("MARTS.FCT_CLINICAL_TRIAL_EARLY_STOPS returned 0 rows; cannot validate.")
            return 2

        # Build validators directly from DataFrames (no DataContext / suites API)
        v_stg = gx.from_pandas(df_stg)
        v_stg.expect_column_values_to_not_be_null("NCT_ID")
        res_stg = v_stg.validate()
        ok_stg = summarize_result("staging:not_null(NCT_ID)", res_stg)

        v_fact = gx.from_pandas(df_fact)
        v_fact.expect_column_values_to_be_between("EARLY_STOP_RATE", min_value=0, max_value=1)
        res_fact = v_fact.validate()
        ok_fact = summarize_result("marts:EARLY_STOP_RATE in [0,1]", res_fact)

        if ok_stg and ok_fact:
            log.info(" Great Expectations checks passed.")
            return 0
        else:
            log.error(" Great Expectations checks FAILED.")
            return 1

    except Exception as e:
        log.error("Failed to run Great Expectations: %s", e)
        log.debug("Traceback:\n%s", traceback.format_exc())
        return 2


if __name__ == "__main__":
    sys.exit(main())