with mpews_raw as (
    select
        acuity_rule_score.*,
        cl_chrg_edit_rule.rule_name,
        cl_chrg_edit_rule.display_name,
        patient_all.pat_key,
        timezone(
            acuity_rule_score.score_calc_utc_dttm,
            'utc',
            'America/New_York'
        )::timestamp as score_calc_local_dttm,
        date_part('minute', score_calc_local_dttm) as minute_of_hour,
        floor(minute_of_hour / 15.0) as quarter_of_hour,
        lpad(quarter_of_hour * 15, 2, '0') as minutes_past_hour,
        (to_char(score_calc_local_dttm, 'YYYY-mm-dd HH24:')
            || minutes_past_hour
            || ':00')::timestamp as score_calc_datetime
    from
        {{source('clarity_ods', 'acuity_rule_score')}} as acuity_rule_score
        inner join {{source('clarity_ods', 'qm_gen_info')}} as qm_gen_info
            on qm_gen_info.registry_data_id = acuity_rule_score.registry_data_id
        inner join {{ref('patient_all')}} as patient_all
            on patient_all.pat_id = qm_gen_info.pat_id
        inner join {{source('clarity_ods', 'cl_chrg_edit_rule')}} as cl_chrg_edit_rule
            on cl_chrg_edit_rule.rule_id = acuity_rule_score.rule_id
    where
        qm_gen_info.acuity_system_id = 100008
),

mpews_pivot as (
    select
        mpews_raw.pat_key,
        mpews_raw.score_calc_datetime,
        max(
            case
                when mpews_raw.rule_name = 'IP CLINICAL DETERIORATION RESPIRATORY RATE'
                then mpews_raw.rule_score
                else 0
            end
        ) as ip_clinical_deterioration_respiratory_rate,
        max(
            case
                when mpews_raw.rule_name = 'IP CLINICAL DETERIORATION OXYGEN SATURATION'
                then mpews_raw.rule_score
                else 0
            end
        ) as ip_clinical_deterioration_oxygen_saturation,
        max(
            case
                when mpews_raw.rule_name = 'IP CLINICAL DETERIORATION CAPILLARY REFILL'
                then mpews_raw.rule_score
                else 0
            end
        ) as ip_clinical_deterioration_capillary_refill,
        max(
            case
                when mpews_raw.rule_name = 'IP CLINICAL DETERIORATION SBP'
                then mpews_raw.rule_score
                else 0
            end
        ) as ip_clinical_deterioration_sbp,
        max(
            case
                when mpews_raw.rule_name = 'IP CLINICAL DETERIORATION HEART RATE'
                then mpews_raw.rule_score
                else 0
            end
        ) as ip_clinical_deterioration_heart_rate,
        max(
            case
                when mpews_raw.rule_name = 'IP CLINICAL DETERIORATION OXYGEN REQUIREMENT'
                then mpews_raw.rule_score
                else 0
            end
        ) as ip_clinical_deterioration_oxygen_requirement,
        max(
            case
                when mpews_raw.rule_name = 'IP CLINICAL DETERIORATION RESPIRATORY EFFORT'
                then mpews_raw.rule_score
                else 0
            end
        ) as ip_clinical_deterioration_respiratory_effort
    from
        mpews_raw
    group by
        mpews_raw.pat_key,
        mpews_raw.score_calc_datetime
)

select
    mpews_pivot.pat_key,
    mpews_pivot.score_calc_datetime,
    mpews_pivot.ip_clinical_deterioration_respiratory_rate,
    mpews_pivot.ip_clinical_deterioration_oxygen_saturation,
    mpews_pivot.ip_clinical_deterioration_capillary_refill,
    mpews_pivot.ip_clinical_deterioration_sbp,
    mpews_pivot.ip_clinical_deterioration_heart_rate,
    mpews_pivot.ip_clinical_deterioration_oxygen_requirement,
    mpews_pivot.ip_clinical_deterioration_respiratory_effort,
    mpews_pivot.ip_clinical_deterioration_respiratory_rate
        + mpews_pivot.ip_clinical_deterioration_oxygen_saturation
        + mpews_pivot.ip_clinical_deterioration_capillary_refill
        + mpews_pivot.ip_clinical_deterioration_sbp
        + mpews_pivot.ip_clinical_deterioration_heart_rate
        + mpews_pivot.ip_clinical_deterioration_oxygen_requirement
        + mpews_pivot.ip_clinical_deterioration_respiratory_effort
    as mpews_total_score
from
    mpews_pivot
