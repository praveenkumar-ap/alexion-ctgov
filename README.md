
# CTGov Data Pipeline – execution Steps

This project ingests Phase 2/3 interventional studies from the ClinicalTrials.gov v2 API, lands the raw JSON in Snowflake (Bronze), flattens it to a staging view (Silver), and builds a small facts table (Gold) with early-stop metrics per sponsor class. The whole thing runs in **GitHub Actions**—no local setup needed.

---

## How to run it

1. **Open GitHub → Actions → “CTGov Ingestion Pipeline” → Run workflow.**

   * This calls the API, inserts raw rows into `CLINICAL_TRIALS_DEV.RAW.RAW_CTGOV_STUDIES`, and then runs dbt to refresh:

     * `STAGING.stg_ctgov_studies`
     * `MARTS.fct_clinical_trial_early_stops`

2. **Open Snowflake**    
   (https://ak93511.ap-southeast-1.snowflakecomputing.com/console/login)
   (credentials provided separately) 
3. Run the checks below.

---

## What to check in Snowflake

```sql
-- Pick a warehouse & the target database
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE CLINICAL_TRIALS_DEV;

-- Bronze: did new rows land?
SELECT COUNT(*) AS row_count FROM RAW.RAW_CTGOV_STUDIES;

-- Last load time (UTC)
SELECT
  MAX(INGESTION_TIMESTAMP) AS last_load_utc,
  CONVERT_TIMEZONE('UTC','Asia/Kolkata', MAX(INGESTION_TIMESTAMP)) AS last_load_ist,
  ANY_VALUE(BATCH_ID) AS sample_batch
FROM RAW.RAW_CTGOV_STUDIES;

-- Peek at flattened Silver view
SELECT * FROM STAGING.stg_ctgov_studies LIMIT 10;

-- Gold: sponsor-class metrics
SELECT * FROM MARTS.fct_clinical_trial_early_stops ORDER BY sponsor_class;

-- Guardrail: early_stop_rate should be within [0,1]
SELECT *
FROM MARTS.fct_clinical_trial_early_stops
WHERE early_stop_rate < 0 OR early_stop_rate > 1;
```

**Expected:**

* Bronze shows a positive `row_count` and a recent `last_load_*`.
* Silver includes columns like `nct_id`, `first_submitted_date`, `study_type`, `phases`, `sponsor_class`.
* Gold has one row per `sponsor_class` with `early_stopped_trials`, `closed_trials`, and `early_stop_rate` between 0 and 1.

---

## What the pipeline does

* **Ingestion (`ingestion/clinical_trials_api.py`)**

  * Filters: Interventional + Phase 2/3 + `StudyFirstSubmitDate >= 2015-01-01`
  * Writes raw JSON to `RAW.RAW_CTGOV_STUDIES (VARIANT)` with `INGESTION_TIMESTAMP` and `BATCH_ID`.

* **Transforms (dbt, `dbt/`)**

  * **Silver**: `STAGING.stg_ctgov_studies` flattens JSON to columns.
  * **Gold**: `MARTS.fct_clinical_trial_early_stops` aggregates:

    * early-stopped trials (Terminated/Withdrawn/Suspended)
    * total closed (Completed + early-stopped)
    * `early_stop_rate = early_stopped / closed`
  * **Tests**: non-null `nct_id`; `early_stop_rate` within \[0,1].

* **Orchestration**: **GitHub Actions** (`.github/workflows/ctgov_ingestion.yml`)

  * Manual “Run workflow” for the demo.
  * A schedule can be enabled if needed (see note below).



## Secrets used by the workflow

These are configured in the repo’s **Actions → Secrets**. Nothing sensitive is committed to code.

* `SNOWFLAKE_ACCOUNT` 
* `SNOWFLAKE_USER`
* `SNOWFLAKE_PASSWORD`
* `SNOWFLAKE_WAREHOUSE` (`TRANSFORM_WH`)
* `SNOWFLAKE_DATABASE` ( `CLINICAL_TRIALS_DEV`)
* `SNOWFLAKE_SCHEMA_RAW` (`RAW`)
* `SNOWFLAKE_SCHEMA_STAGING` (`STAGING`)
* `SNOWFLAKE_SCHEMA_MARTS` (`MARTS`)

The workflow exports these as env vars for both the ingestion script and dbt.

