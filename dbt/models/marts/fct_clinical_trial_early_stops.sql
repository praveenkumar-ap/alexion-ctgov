{{ config(materialized='table') }}

with base as (
  select
    coalesce(sponsor_class, 'UNKNOWN') as sponsor_class,
    upper(latest_overall_status)       as status
  from {{ ref('stg_ctgov_studies') }}
),

flags as (
  select
    sponsor_class,
    status,
    case when status in ('TERMINATED','WITHDRAWN','SUSPENDED') then 1 else 0 end as is_early_stop,
    case when status in ('COMPLETED','TERMINATED','WITHDRAWN','SUSPENDED') then 1 else 0 end as is_closed
  from base
)

select
  sponsor_class,
  sum(is_early_stop) as early_stop_trials,
  sum(is_closed) as closed_trials,
  case
    when sum(is_closed) = 0 then 0
    else (1.0 * sum(is_early_stop)) / sum(is_closed)
  end  as early_stop_rate
from flags
group by 1
order by 1