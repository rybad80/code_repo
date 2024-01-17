{{ config(meta = {
    'critical': true
}) }}

with hppd_numerators as (
    select
        'HPPDratio' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        numerator
    from {{ref('stg_nursing_hppd_w1_hours')}}
    where metric_abbreviation = 'HPPDhours'

    union

    select
        'HPPD_RN' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        numerator
    from {{ref('stg_nursing_hppd_w1_hours')}}
    where metric_abbreviation = 'RNhours'
),

fraction as (
    select
        hppd_numerators.metric_abbreviation,
        hppd_numerators.metric_dt_key,
        null as worker_id,
        hppd_numerators.cost_center_id,
        null as cost_center_site_id,
        null as job_code,
        hppd_numerators.job_group_id,
        hppd_numerators.metric_grouper,
        hppd_numerators.numerator,
        sum(stg_nursing_unit_w1_patient_days.numerator) as denominator
    from hppd_numerators
    inner join {{ref('stg_nursing_unit_w1_patient_days')}} as stg_nursing_unit_w1_patient_days
        on hppd_numerators.cost_center_id = stg_nursing_unit_w1_patient_days.cost_center_id
        and hppd_numerators.metric_dt_key = stg_nursing_unit_w1_patient_days.metric_dt_key
        and stg_nursing_unit_w1_patient_days.metric_abbreviation = 'PatDaysPPactualTot'
    group by
        hppd_numerators.metric_abbreviation,
        hppd_numerators.metric_dt_key,
        hppd_numerators.cost_center_id,
        hppd_numerators.job_group_id,
        hppd_numerators.metric_grouper,
        hppd_numerators.numerator
)

select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    case when denominator is not null
        and denominator != 0
        then round(numerator / denominator, 1)
            end as row_metric_calculation
from fraction
