{{ config(meta = {
    'critical': true
}) }}

with hppd_ratio as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        numerator,
        denominator,
        row_metric_calculation as uap_minuend
    from {{ref('stg_nursing_hppd_w2_ratio')}}
    where
        metric_abbreviation = 'HPPDratio'
        and denominator is not null
        and denominator != 0
),

rn_ratio as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        numerator,
        denominator,
        row_metric_calculation as uap_subtrahend
    from {{ref('stg_nursing_hppd_w2_ratio')}}
    where
        metric_abbreviation = 'HPPD_RN'
        and denominator is not null
        and denominator != 0
)

select
    'HPPD_UAP' as metric_abbreviation,
    hppd_ratio.metric_dt_key,
    null as worker_id,
    hppd_ratio.cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    'UAP' as job_group_id,
    'UAP' as metric_grouper,
    hppd_ratio.uap_minuend - coalesce(rn_ratio.uap_subtrahend, 0) as numerator,
    null::numeric as denominator,
    hppd_ratio.uap_minuend - coalesce(rn_ratio.uap_subtrahend, 0) as row_metric_calculation
from hppd_ratio
left join rn_ratio on hppd_ratio.metric_dt_key = rn_ratio.metric_dt_key
    and hppd_ratio.cost_center_id = rn_ratio.cost_center_id
