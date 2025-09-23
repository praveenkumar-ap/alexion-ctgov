-- dbt/models/staging/stg_ctgov_phases_csv.sql
{{ config(materialized='view') }}

with base as (
  select nct_id, phases_json
  from {{ ref('stg_ctgov_studies') }}
)
select
  b.nct_id,
  listagg(f.value::string, ',') within group (order by f.index) as phases_csv
from base b,
     lateral flatten(input => b.phases_json, outer => true) f
group by b.nct_id
