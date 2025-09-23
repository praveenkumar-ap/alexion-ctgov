{{ config(materialized='view') }}

with src as (
  select RAW_DATA, INGESTION_TIMESTAMP, BATCH_ID
  from {{ source('raw', 'ctgov_studies') }}
),

final as (
  select
    RAW_DATA:"protocolSection":"identificationModule":"nctId"::string       as nct_id,
    RAW_DATA:"protocolSection":"identificationModule":"briefTitle"::string  as brief_title,
    RAW_DATA:"protocolSection":"statusModule":"overallStatus"::string       as latest_overall_status,
    coalesce(
      RAW_DATA:"protocolSection":"statusModule":"studyFirstSubmitDateStruct":"date"::date,
      RAW_DATA:"protocolSection":"statusModule":"studyFirstSubmitDate"::date
    )                                                                       as first_submitted_date,
    RAW_DATA:"protocolSection":"designModule":"studyType"::string           as study_type,

    CASE
      WHEN TYPEOF(RAW_DATA:"protocolSection":"designModule":"phases") = 'ARRAY'
        THEN ARRAY_TO_STRING(RAW_DATA:"protocolSection":"designModule":"phases", ',')
      WHEN TYPEOF(RAW_DATA:"protocolSection":"designModule":"phases") = 'VARCHAR'
        THEN RAW_DATA:"protocolSection":"designModule":"phases"::string
      ELSE NULL
    END                                                                     as phases,

    coalesce(
      RAW_DATA:"protocolSection":"sponsorCollaboratorsModule":"leadSponsor":"class"::string,
      RAW_DATA:"protocolSection":"sponsorCollaboratorsModule":"leadSponsor":"agencyClass"::string
    )                                                                       as sponsor_class,

    RAW_DATA:"hasResults"::boolean                                          as has_results,
    INGESTION_TIMESTAMP,
    BATCH_ID
  from src
)

select * from final