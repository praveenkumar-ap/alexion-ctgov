# CTGov Data Pipeline — how to run it, what to check

This repo pulls **Phase 2/3 interventional** studies from the ClinicalTrials.gov v2 API, lands the **raw JSON** in Snowflake (**Bronze**), flattens it into a clean **staging view** (**Silver**), and builds a **fact table** with early-stop metrics (**Gold**).
Everything runs in **GitHub Actions**, so don’t need local setup for quick review and execution

---

## What architecture did I use?

**Medallion (Bronze → Silver → Gold)**

* **Bronze – RAW landing** *(Snowflake: `CLINICAL_TRIALS_DEV.RAW.RAW_CTGOV_STUDIES`)*
  Store the unmodified API JSON as `VARIANT` + `INGESTION_TIMESTAMP` + `BATCH_ID`. It’s append-only and audit-friendly.

* **Silver – Staging** *(Snowflake view: `CLINICAL_TRIALS_DEV.STAGING.stg_ctgov_studies`)*
  Flatten JSON into tidy columns: `nct_id`, `brief_title`, `latest_overall_status`, `first_submitted_date`, `study_type`, `phases`, `sponsor_class`, `has_results`.

* **Gold – Marts** *(Snowflake table: `CLINICAL_TRIALS_DEV.MARTS.fct_clinical_trial_early_stops`)*
  One row per `sponsor_class` with:

  * `early_stopped` = Terminated / Withdrawn / Suspended
  * `closed` = Completed + early\_stopped
  * `early_stop_rate` = early\_stopped / closed

### Extra bits I added (and why)

* **Great Expectations (GX)**: quick data checks (row counts present, key fields present). This catches bad loads early and fails the workflow if expectations are not met.
* **Data quality tests (dbt)**: schema tests (e.g., non-null `nct_id`, `early_stop_rate` [0,1]) to guard core assumptions in the Gold layer.
* **CDC-style visibility**: every load gets a `BATCH_ID`, to compare latest vs previous loads (row deltas, new NCT IDs). 

---

## How to run the workflow

# 1. GitHub → Actions → “CTGov Ingestion Pipeline” → Run workflow.

#### or

#### Direct work flow link (https://github.com/praveenkumar-ap/alexion-ctgov/actions/workflows/ctgov_ingestion.yml)

*Right click the link and open new tab so that ReadME page reamins here.*

**_Note: The workflow take 6-7 mins to finish, most of the time time takes to establish snowflake connection and save the data to table_**

   
<img width="384" height="125" alt="Screenshot 2025-09-24 at 4 13 32 PM" src="https://github.com/user-attachments/assets/b4378c5c-d111-4199-b43b-843bf2c146a3" />
<img width="1426" height="195" alt="Screenshot 2025-09-24 at 4 14 09 PM" src="https://github.com/user-attachments/assets/7cbb52ad-a3f1-4a0c-a220-90767aa016dc" />

   The workflow:

   * runs **ingestion** (`ingestion/clinical_trials_api.py`) and inserts into `RAW.RAW_CTGOV_STUDIES`
   * runs **dbt** to build `STAGING.stg_ctgov_studies` and `MARTS.fct_clinical_trial_early_stops`
   * runs **Great Expectations** and **dbt tests**
   * builds **dbt docs** and uploads an artifact (`dbt-docs`)

# 2. Click Snowflake console below and run the checks as follows.

*Right click the link and open new tab so that ReadME page reamins here.*

*Link to snowflake workspace*  <a href="https://app.snowflake.com/wcofxcf/no82177/#/workspaces/ws/USER%24/PUBLIC/DEFAULT%24" target="_blank" rel="noopener noreferrer">View Snowflake Workspace</a>



*Credentials  are mailed*

> As per assessment workflow is set to runs for “every 2 hours on the first Monday”. Also we can run the workflow manually for test

---

## What to check in Snowflake (start here)

```sql
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE CLINICAL_TRIALS_DEV;
```

### 1) Bronze — did new rows land?

```sql
-- Total rows in Bronze
SELECT COUNT(*) AS row_count
FROM RAW.RAW_CTGOV_STUDIES;

-- Most recent load time + a sample batch
SELECT
  MAX(INGESTION_TIMESTAMP)                                                 AS last_load_utc,
  CONVERT_TIMEZONE('UTC','Asia/Kolkata', MAX(INGESTION_TIMESTAMP))         AS last_load_ist,
  ANY_VALUE(BATCH_ID)                                                      AS sample_batch
FROM RAW.RAW_CTGOV_STUDIES;

-- check  parsed JSON (NCT ID, status, type, phases)
SELECT
  RAW_DATA:"protocolSection":"identificationModule":"nctId"::string       AS nct_id,
  RAW_DATA:"protocolSection":"statusModule":"overallStatus"::string       AS overall_status,
  RAW_DATA:"protocolSection":"designModule":"studyType"::string           AS study_type,
  RAW_DATA:"protocolSection":"designModule":"phases"                      AS phases_json,
  INGESTION_TIMESTAMP,
  BATCH_ID
FROM RAW.RAW_CTGOV_STUDIES
ORDER BY INGESTION_TIMESTAMP DESC
LIMIT 15;

-- Distinct batches (newest first)
WITH ranked AS (
  SELECT BATCH_ID, MAX(INGESTION_TIMESTAMP) AS ts
  FROM RAW.RAW_CTGOV_STUDIES
  GROUP BY BATCH_ID
)
SELECT BATCH_ID, ts
FROM ranked
ORDER BY ts DESC;
```

**Expected:** A recent timestamp, at least one batch, and a non-zero row count.

---

### 2) Silver — JSON flattened correctly?

```sql
-- Peek at flattened rows
SELECT *
FROM STAGING.stg_ctgov_studies
LIMIT 10;

-- Assessment filter sanity: interventional only
SELECT DISTINCT study_type
FROM STAGING.stg_ctgov_studies;

-- Phase 2 or 3 presence (string contains PHASE2/PHASE3)
SELECT COUNT(*) AS p2_p3_rows
FROM STAGING.stg_ctgov_studies
WHERE REGEXP_LIKE(phases_json, 'PHASE2|PHASE3');

-- How many rows have first_submitted_date populated
SELECT
  COUNT(*) AS total_rows,
  COUNT(first_submitted_date) AS populated_first_submitted_date
FROM STAGING.stg_ctgov_studies;
```

---

### 3) Gold — early-stop metrics per sponsor class

```sql
SELECT
  sponsor_class,
  EARLY_STOP_TRIALS,
  CLOSED_TRIALS,
  early_stop_rate
FROM MARTS.fct_clinical_trial_early_stops
ORDER BY sponsor_class;


-- Order by sponser class
SELECT
  sponsor_class,
  EARLY_STOP_TRIALS AS early_stopped_trials,
  CLOSED_TRIALS AS closed_trials,
  early_stop_rate
FROM MARTS.fct_clinical_trial_early_stops
ORDER BY sponsor_class;

--Cross-check against Silver

SELECT
  sponsor_class,
  COUNT_IF(latest_overall_status IN ('TERMINATED','WITHDRAWN','SUSPENDED')) AS early_stopped_trials,
  COUNT_IF(latest_overall_status IN ('COMPLETED','TERMINATED','WITHDRAWN','SUSPENDED')) AS closed_trials
FROM STAGING.stg_ctgov_studies
GROUP BY sponsor_class
ORDER BY sponsor_class;


--Top statuses within Phase 3 only
-- If you have phases CSV:
SELECT latest_overall_status, COUNT(*) AS cnt
FROM STAGING.stg_ctgov_studies
WHERE phases_json ILIKE '%PHASE3%'
GROUP BY latest_overall_status
ORDER BY cnt DESC;

```

**Required guardrail:**

```sql
-- early_stop_rate must be between 0 and 1
SELECT *
FROM MARTS.fct_clinical_trial_early_stops
WHERE early_stop_rate < 0 OR early_stop_rate > 1;  -- expect 0 rows
```

---

## CDC-style comparisons 
```sql
-- Identify latest and previous batch
WITH ranked AS (
  SELECT BATCH_ID, MAX(INGESTION_TIMESTAMP) AS ts
  FROM RAW.RAW_CTGOV_STUDIES
  GROUP BY BATCH_ID
),
top2 AS (
  SELECT BATCH_ID, ts, ROW_NUMBER() OVER (ORDER BY ts DESC) AS rn
  FROM ranked
)
SELECT * FROM top2 WHERE rn <= 2;
```

```sql
-- Row counts for those 2 batches
WITH ranked AS (
  SELECT BATCH_ID, MAX(INGESTION_TIMESTAMP) AS ts
  FROM RAW.RAW_CTGOV_STUDIES
  GROUP BY BATCH_ID
),
latest AS (SELECT BATCH_ID FROM ranked ORDER BY ts DESC LIMIT 1),
previous AS (SELECT BATCH_ID FROM ranked ORDER BY ts DESC LIMIT 1 OFFSET 1)
SELECT
  (SELECT COUNT(*) FROM RAW.RAW_CTGOV_STUDIES WHERE BATCH_ID = (SELECT BATCH_ID FROM latest))   AS latest_rows,
  (SELECT COUNT(*) FROM RAW.RAW_CTGOV_STUDIES WHERE BATCH_ID = (SELECT BATCH_ID FROM previous)) AS previous_rows;
```

```sql
-- New NCT IDs in the latest batch (not seen in previous)
WITH ranked AS (
  SELECT BATCH_ID, MAX(INGESTION_TIMESTAMP) AS ts
  FROM RAW.RAW_CTGOV_STUDIES
  GROUP BY BATCH_ID
),
b AS (
  SELECT BATCH_ID, ts, ROW_NUMBER() OVER (ORDER BY ts DESC) AS rn
  FROM ranked
),
latest AS (SELECT BATCH_ID FROM b WHERE rn = 1),
prev   AS (SELECT BATCH_ID FROM b WHERE rn = 2),
latest_ids AS (
  SELECT DISTINCT RAW_DATA:"protocolSection":"identificationModule":"nctId"::string AS nct_id
  FROM RAW.RAW_CTGOV_STUDIES
  WHERE BATCH_ID = (SELECT BATCH_ID FROM latest)
),
prev_ids AS (
  SELECT DISTINCT RAW_DATA:"protocolSection":"identificationModule":"nctId"::string AS nct_id
  FROM RAW.RAW_CTGOV_STUDIES
  WHERE BATCH_ID = (SELECT BATCH_ID FROM prev)
)
SELECT l.nct_id
FROM latest_ids l
LEFT JOIN prev_ids p USING (nct_id)
WHERE p.nct_id IS NULL;
```

---

## Data quality & documentation

* **dbt tests**

  * `stg_ctgov_studies.nct_id` is **not null**
  * `fct_clinical_trial_early_stops.early_stop_rate` **between 0 and 1**
    Tests run inside the workflow; failures fail the run.

* **Great Expectations**
  Basic checks on Bronze/Silver (non-empty loads, key fields present). If a check fails, the run fails.

* **dbt docs (persist\_docs)**
  Field and model descriptions are persisted. The workflow uploads a **`dbt-docs`** artifact—download it from the completed run to review model documentation.


## What the ingestion script filters

* `StudyType = INTERVENTIONAL`
* `Phase in (PHASE2, PHASE3)`
* `StudyFirstSubmitDate >= 2015-01-01`

It lands the full JSON in **Bronze**, **Silver** and **Gold** are rebuilt by dbt.

---
