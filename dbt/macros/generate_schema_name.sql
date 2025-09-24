-- Fails if any row violates the rule
select *
from {{ ref('fct_clinical_trial_early_stops') }}
where early_stop_rate < 0 or early_stop_rate > 1
