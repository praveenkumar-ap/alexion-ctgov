#!/usr/bin/env python3
"""
Great Expectations validation for the STAGING.stg_ctgov_studies view in Snowflake.
"""

import json
import logging
import os
import sys
from datetime import date
from typing import Tuple
from urllib.parse import quote

import great_expectations as gx

# -------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("gx")


def _need(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _conn_parts() -> Tuple[str, str, str, str, str, str, str]:
    """
    Collect and normalize Snowflake connection parameters.
    Returns (account, user, password, warehouse, database, schema, role)
    """
    account = _need("SNOWFLAKE_ACCOUNT").strip()
    user = _need("SNOWFLAKE_USER").strip()
    password = _need("SNOWFLAKE_PASSWORD")
    wh = os.getenv("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH").strip()
    db = os.getenv("SNOWFLAKE_DATABASE", "CLINICAL_TRIALS_DEV").strip()
    sc = os.getenv("SNOWFLAKE_SCHEMA", "STAGING").strip()
    role = os.getenv("SNOWFLAKE_ROLE", "").strip() or None
    return account, user, password, wh, db, sc, role


def _build_sqlalchemy_url() -> str:
    account, user, pwd, wh, db, sc, role = _conn_parts()

    # URL-encode user/pwd to handle special chars
    user_q = quote(user, safe="")
    pwd_q = quote(pwd, safe="")

    base = f"snowflake://{user_q}:{pwd_q}@{account}/{db}/{sc}?warehouse={wh}"
    if role:
        base += f"&role={quote(role, safe='')}"
    return base


def _table_from_cli(default_schema: str) -> Tuple[str, str]:
    """
    Allow optional override via CLI:
      python run_gx_checks.py --table STAGING.STG_CTGOV_STUDIES
    Returns (schema, table)
    """
    schema = default_schema.upper()
    table = "STG_CTGOV_STUDIES"
    argv = sys.argv[1:]
    if "--table" in argv:
        idx = argv.index("--table")
        try:
            full = argv[idx + 1]
        except IndexError:
            raise SystemExit("ERROR: --table must be followed by SCHEMA.TABLE")
        parts = full.split(".")
        if len(parts) != 2:
            raise SystemExit("ERROR: --table must be SCHEMA.TABLE")
        schema, table = parts[0].upper(), parts[1].upper()
    return schema, table


def main() -> None:
    log.info("Starting Great Expectations validation…")

    try:
        url = _build_sqlalchemy_url()
        _, _, _, _, db, sc, _ = _conn_parts()
        log.info("Target: %s.%s via Snowflake SQLAlchemy", db, sc)
    except Exception as e:
        log.error("Config error: %s", e)
        sys.exit(2)

    # Table override (optional)
    try:
        schema_override, table = _table_from_cli(sc)
        # If user passed a different schema, we need to rebuild the URL with that schema
        if schema_override != sc:
            os.environ["SNOWFLAKE_SCHEMA"] = schema_override
            url = _build_sqlalchemy_url()
            sc = schema_override
            log.info("Using overridden schema for validation: %s", sc)
    except SystemExit as se:
        log.error(str(se))
        sys.exit(2)

    log.debug("SQLAlchemy URL (safe): snowflake://<user>:<password>@%s/%s/%s?warehouse=…", *url.split("@")[1].split("/")[0:1],)

    # Initialize GE context/datasource
    try:
        context = gx.get_context()
        ds = context.sources.add_sql(name="sf", connection_string=url)
        asset = ds.add_table_asset(name="staging_asset", table_name=table)
        batch_request = asset.build_batch_request()
        suite = context.suites.add("stg_ctgov_studies_suite", overwrite_existing=True)
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite.name,
        )
        log.info("Connected and initialized validator for %s.%s.%s", db, sc, table)
    except Exception as e:
        log.error("Failed to initialize Great Expectations with Snowflake: %s", e)
        sys.exit(3)

    # ---------------- Expectations ----------------
    try:
        # Meta health
        validator.expect_table_row_count_to_be_greater_than(0)

        # Keys
        validator.expect_column_values_to_not_be_null("NCT_ID")
        validator.expect_column_values_to_match_regex("NCT_ID", r"^NCT\d{8}$")
        validator.expect_column_values_to_be_unique("NCT_ID")  # CDC guard

        # Business constraints
        validator.expect_column_values_to_be_in_set("STUDY_TYPE", ["INTERVENTIONAL"])
        validator.expect_column_values_to_match_regex("PHASES", r"(PHASE2|PHASE3)")

        # Status sanity (Snowflake uppercased by default)
        validator.expect_column_values_to_be_in_set(
            "LATEST_OVERALL_STATUS",
            [
                "COMPLETED",
                "TERMINATED",
                "WITHDRAWN",
                "SUSPENDED",
                "ACTIVE_NOT_RECRUITING",
                "ENROLLING_BY_INVITATION",
                "NOT_YET_RECRUITING",
                "RECRUITING",
                "AVAILABLE",
                "NO_LONGER_AVAILABLE",
                "TEMPORARILY_NOT_AVAILABLE",
                "APPROVED_FOR_MARKETING",
                "WITHHELD",
                "UNKNOWN",
            ],
        )

        # Date gate (≥ 2015-01-01). Nulls pass this one; use not_null above to hard-enforce presence if needed.
        validator.expect_column_values_to_be_between(
            "FIRST_SUBMITTED_DATE",
            min_value=date(2015, 1, 1),
            parse_strings_as_datetimes=True,
        )
    except Exception as e:
        log.error("Failed while defining expectations: %s", e)
        sys.exit(4)

    # ---------------- Validate & report ----------------
    try:
        context.suites.add_or_update(validator.expectation_suite)
        res = validator.validate()
    except Exception as e:
        log.error("Validation execution failed: %s", e)
        sys.exit(5)

    # Ensure reports dir
    os.makedirs("quality/.reports", exist_ok=True)

    # JSON artifact
    json_path = "quality/.reports/gx_result.json"
    try:
        with open(json_path, "w") as f:
            json.dump(res.to_json_dict(), f)
        log.info("Wrote JSON results → %s", json_path)
    except Exception as e:
        log.warning("Could not write JSON results: %s", e)

    #  summary
    summary_path = "quality/.reports/gx_summary.txt"
    try:
        stats = res.statistics
        failed = []
        for ev in res.results or []:
            success = ev.get("success", True)
            if not success:
                expect = ev.get("expectation_config", {}).get("expectation_type", "unknown")
                failed.append(expect)

        lines = []
        lines.append(f"Suite: {res.meta.get('expectation_suite_name', 'n/a')}")
        lines.append(f"Success: {res.success}")
        lines.append(f"Evaluated: {stats.get('evaluated_expectations', 0)}  |  "
                    f"Successful: {stats.get('successful_expectations', 0)}  |  "
                    f"Unsuccessful: {stats.get('unsuccessful_expectations', 0)}")
        if failed:
            lines.append("Failed expectations:")
            for e in failed:
                lines.append(f"  - {e}")

        with open(summary_path, "w") as f:
            f.write("\n".join(lines) + "\n")

        #  summary to logs
        log.info("Validation summary: success=%s, failed=%d",
                 res.success, stats.get("unsuccessful_expectations", 0))
        if failed:
            log.warning("Failed expectations: %s", ", ".join(failed))
    except Exception as e:
        log.warning("Could not write text summary: %s", e)

    if not res.success:
        log.error("Great Expectations validation FAILED")
        sys.exit(1)

    log.info("Great Expectations validation PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Top-level safety net
        log.exception("Unexpected error: %s", e)
        sys.exit(99)
