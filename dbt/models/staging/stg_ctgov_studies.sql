
{{ config(materialized='view') }}

with src as (
  select RAW_DATA, INGESTION_TIMESTAMP, BATCH_ID
  from {{ ref('bronze_ctgov_studies_latest') }}
)

select
  -- identifiers & headline
  RAW_DATA:"protocolSection":"identificationModule":"nctId"::string as nct_id,
  RAW_DATA:"protocolSection":"identificationModule":"briefTitle"::string  as brief_title,

  -- status
  RAW_DATA:"protocolSection":"statusModule":"overallStatus"::string as latest_overall_status,

  -- FIRST SUBMITTED DATE (handle all variants seen in v2)
  coalesce(
    try_to_date(RAW_DATA:"protocolSection":"statusModule":"studyFirstSubmitDateStruct":"date"::string),
    try_to_date(RAW_DATA:"protocolSection":"statusModule":"studyFirstSubmitDate"::string),
    try_to_date(RAW_DATA:"protocolSection":"statusModule":"firstSubmittedDateStruct":"date"::string),
    try_to_date(RAW_DATA:"protocolSection":"statusModule":"firstSubmittedDate"::string)
  ) as first_submitted_date,

  -- design
  RAW_DATA:"protocolSection":"designModule":"studyType"::string as study_type,

  -- leave as JSON (VARIANT) to avoid array/string casting issues
  RAW_DATA:"protocolSection":"designModule":"phases" as phases_json,

  -- sponsor class (API may use class or agencyClass depending on record)
  coalesce(
    RAW_DATA:"protocolSection":"sponsorCollaboratorsModule":"leadSponsor":"class"::string,
    RAW_DATA:"protocolSection":"sponsorCollaboratorsModule":"leadSponsor":"agencyClass"::string
  ) as sponsor_class,

  RAW_DATA:"hasResults"::boolean as has_results,
  INGESTION_TIMESTAMP,
  BATCH_ID
from src
