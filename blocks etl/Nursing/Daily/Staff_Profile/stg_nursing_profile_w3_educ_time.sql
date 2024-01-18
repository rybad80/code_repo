/* stg_nursing_profile_w3_educ_time
write metric rows for the metrics driven off elapsed time
of educational degrees related to nursing
sourced from stg_nursing_educ_p4_time
for example: years since nursing degree,
nursing as a second degree
*/
with
metric_row_set as (
    select
        metric_abbreviation,
        0 as metric_dt_key,
        worker_id,
        profile_name,
        metric_grouper,
        numerator
    from
        {{ ref('stg_nursing_educ_p4_time') }}
)

select
    metric_row_set.metric_abbreviation,
    case metric_row_set.metric_dt_key
        when 0 then nursing_pay_period.pp_end_dt_key
        else metric_row_set.metric_dt_key
    end as metric_dt_key,
    metric_row_set.worker_id,
    metric_row_set.profile_name,
    metric_row_set.metric_grouper,
    metric_row_set.numerator
from
    metric_row_set
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on metric_row_set.metric_dt_key < nursing_pay_period.pp_end_dt_key
        and nursing_pay_period.latest_pay_period_ind = 1
