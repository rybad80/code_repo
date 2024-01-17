/* stg_nursing_staff_w9_employment
Gather the Nursing Dashboard's Employment sheet metric rows into the staffing
wave that supports the hire, terminations, KPIs, and related waterfall components
for the employement deltas over the last 30 days and 365 days.
Except for the monthly, the metric_dt_key represents the current data date
and the monthly metrics use the final pay period of the month (in which the hire or
termination event occurrred).
*/

select /* for the monthly hire, termination run charts */
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
   {{ ref('stg_nursing_staff_p7c_rn_monthly') }}

union all
select /* for the voluntary/involuntary term breakouts */
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
   {{ ref('stg_nursing_staff_p7d_termination_type') }}

union all

select /* for the hire KPIs and waterfall bars */
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
   {{ ref('stg_nursing_staff_p7e_hire_total') }}

union all
select /* for the termination KPIs and waterfall bars */
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
   {{ ref('stg_nursing_staff_p7f_termination_total') }}

union all

select /* for the % new RNs */
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
   {{ ref('stg_nursing_staff_p7g_percent') }}
