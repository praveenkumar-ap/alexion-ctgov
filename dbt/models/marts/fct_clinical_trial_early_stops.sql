{{ config(materialized='table') }}

with base as (
  select *
  from {{ ref('stg_ctgov_studies') }}
  where study_type = 'INTERVENTIONAL'
    and (phases ilike '%PHASE2%' or phases ilike '%PHASE3%')
),

agg as (
  select
    coalesce(sponsor_class, 'UNKNOWN') as sponsor_class,
    sum(case when latest_overall_status in ('TERMINATED','WITHDRAWN','SUSPENDED') then 1 else 0 end) as early_stop_cnt,
    sum(case when latest_overall_status in ('COMPLETED','TERMINATED','WITHDRAWN','SUSPENDED') then 1 else 0 end) as closed_cnt
  from base
  group by 1
)

select
  sponsor_class,
  early_stop_cnt,
  closed_cnt,
  case when closed_cnt = 0 then null else early_stop_cnt::float/closed_cnt::float end as early_stop_rate
from agg