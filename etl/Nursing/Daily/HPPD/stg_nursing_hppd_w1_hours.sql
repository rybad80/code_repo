{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_hppd_w1_hours
gathers the timereporting productive direct (regular and overtime) time
only for the hospital room and board (and obervation) units
summing up by the RN (Acute only direct patient care) & UAP rollups
so that the unit RN/UAP skill mix ratios can be compared to target and that overall usage
of these variable job roles per the actual patient days also can be compared
to target
Note:  the safety observation hours by UAPa are not counted for this HPPD productive direct time
*/

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    null as  job_group_id,
    metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    {{ ref('stg_nccs_hppd_p4_use_direct_hours') }}
