{{ config(meta = {
    'critical': true
}) }}

with cc_pp_patient_days as (
    select
        metric_dt_key,
        cost_center_id,
        sum(numerator) as patient_days
    from {{ ref('stg_nursing_unit_w1_patient_days') }}
    where
        metric_abbreviation = 'PatDaysPPactualTot'
        and numerator is not null
        and numerator != 0
    group by
        metric_dt_key,
        cost_center_id
),

hppd_gap_calc as (
    select
        case stg_nursing_hppd_w4_variance.metric_abbreviation
        when 'HPPDvarRN' then 'HPPDgapRN'
        when 'HPPDvarUAP' then 'HPPDgapUAP'
        when 'HPPDvar' then 'HPPDgap'
        else 'HPPDgapUNK' end as metric_abbreviation,
        stg_nursing_hppd_w4_variance.metric_dt_key,
        stg_nursing_hppd_w4_variance.cost_center_id,
        stg_nursing_hppd_w4_variance.job_group_id,
        stg_nursing_hppd_w4_variance.metric_grouper,
        round((stg_nursing_hppd_w4_variance.row_metric_calculation
                * cc_pp_patient_days.patient_days) / 80, 1) as numerator
    from {{ ref('stg_nursing_hppd_w4_variance') }} as stg_nursing_hppd_w4_variance
    inner join cc_pp_patient_days
        on stg_nursing_hppd_w4_variance.metric_dt_key = cc_pp_patient_days.metric_dt_key
        and stg_nursing_hppd_w4_variance.cost_center_id = cc_pp_patient_days.cost_center_id
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    hppd_gap_calc
