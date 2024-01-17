{{ config(meta = {
    'critical': true
}) }}
/* stg_nursing_hppd_w4_variance
now that each HPPD ratio is calculated, compare to the overall and the subset
targets, and also calculate the actual RN to UAP skill mix for the period
*/
with skillmix_hppdpart as (
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
        row_metric_calculation
    from {{ ref('stg_nursing_hppd_w2_ratio') }}
    where metric_abbreviation = 'HPPD_RN'

    union all

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
        row_metric_calculation
    from {{ ref('stg_nursing_hppd_w3_uap') }}
    where metric_abbreviation = 'HPPD_UAP'
),

rn_uap_targets as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        row_metric_calculation as cc_hppd_target
    from {{ ref('stg_nursing_target_period_workforce') }}
    where
        metric_grouper in ('RN', 'UAP')
        and metric_abbreviation in ('HPPDtrgtRN', 'HPPDtrgtUAP')
),

rn_uap_ratios as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        row_metric_calculation as cc_hppd_ratio
    from {{ ref('stg_nursing_hppd_w2_ratio') }}
    where
        metric_abbreviation = 'HPPD_RN'
        and denominator is not null
        and denominator != 0

    union all

    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        row_metric_calculation as cc_hppd_ratio
    from {{ ref('stg_nursing_hppd_w3_uap') }}

)

    select
        case when rn_uap_ratios.metric_grouper = 'RN'
            then 'HPPDvarRN'
            else 'HPPDvarUAP' end as metric_abbreviation,
        rn_uap_ratios.metric_dt_key,
        null as worker_id,
        rn_uap_ratios.cost_center_id,
        null as cost_center_site_id,
        null as job_code,
        rn_uap_ratios.job_group_id,
        rn_uap_ratios.metric_grouper,
        coalesce(rn_uap_targets.cc_hppd_target, 0) - rn_uap_ratios.cc_hppd_ratio as numerator,
        null::numeric as denominator,
        coalesce(rn_uap_targets.cc_hppd_target, 0) - rn_uap_ratios.cc_hppd_ratio as row_metric_calculation
    from rn_uap_ratios
    left join rn_uap_targets on rn_uap_ratios.metric_dt_key = rn_uap_targets.metric_dt_key
        and rn_uap_ratios.cost_center_id = rn_uap_targets.cost_center_id
        and rn_uap_ratios.metric_grouper = rn_uap_targets.metric_grouper

    union all

    select
        'HPPDvar' as metric_abbreviation,
        stg_nursing_hppd_w2_ratio.metric_dt_key,
        null as worker_id,
        stg_nursing_hppd_w2_ratio.cost_center_id,
        null as cost_center_site_id,
        null as job_code,
        null as job_group_id,
        null as metric_grouper,
        coalesce(stg_nursing_target_period_workforce.numerator, 0)
            - stg_nursing_hppd_w2_ratio.row_metric_calculation as hppd_ratio_var_numerator,
        null::numeric as denominator,
        hppd_ratio_var_numerator as row_metric_calculation
    from {{ ref('stg_nursing_hppd_w2_ratio') }} as stg_nursing_hppd_w2_ratio
    left join {{ ref('stg_nursing_target_period_workforce') }} as stg_nursing_target_period_workforce
        on stg_nursing_hppd_w2_ratio.metric_dt_key = stg_nursing_target_period_workforce.metric_dt_key
        and stg_nursing_hppd_w2_ratio.cost_center_id = stg_nursing_target_period_workforce.cost_center_id
        and stg_nursing_target_period_workforce.metric_abbreviation = 'HPPDtrgt'
        and stg_nursing_target_period_workforce.metric_grouper is null
    where stg_nursing_hppd_w2_ratio.metric_abbreviation = 'HPPDratio'
        and stg_nursing_hppd_w2_ratio.metric_grouper is null

    union all

    select
        case skillmix_hppdpart.metric_abbreviation
            when 'HPPD_RN' then 'RNskillmixPct'
            else 'UAPskillmixPct' end as metric_abbreviation,
        skillmix_hppdpart.metric_dt_key,
        skillmix_hppdpart.worker_id,
        skillmix_hppdpart.cost_center_id,
        skillmix_hppdpart.cost_center_site_id,
        skillmix_hppdpart.job_code,
        skillmix_hppdpart.job_group_id,
        skillmix_hppdpart.metric_grouper,
        skillmix_hppdpart.row_metric_calculation as hppdpart,
        stg_nursing_hppd_w2_ratio.row_metric_calculation as hppdoverall,
        round(hppdpart / hppdoverall, 4) as row_metric_calculation
    from skillmix_hppdpart
    inner join {{ ref('stg_nursing_hppd_w2_ratio') }} as stg_nursing_hppd_w2_ratio
        on skillmix_hppdpart.cost_center_id = stg_nursing_hppd_w2_ratio.cost_center_id
        and skillmix_hppdpart.metric_dt_key = stg_nursing_hppd_w2_ratio.metric_dt_key
        and stg_nursing_hppd_w2_ratio.metric_abbreviation = 'HPPDratio'
        and stg_nursing_hppd_w2_ratio.row_metric_calculation > 0
