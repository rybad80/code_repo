with risk_score as ( -- Link control risk and complications risk rule_id back to unique patient
    select
        stg_diabetes_patient.diabetes_reporting_month,
        registry_data_info.pat_key,
        registry_metric_history.metric_string_value as risk_score,
        case
            when
                registry_metric_history.mstr_chrg_edit_rule_key = '85014'
            then registry_metric_history.metric_last_upd_dt
        end as control_risk_upd_dt,
        case
            when
                registry_metric_history.mstr_chrg_edit_rule_key = '78730'
            then registry_metric_history.metric_last_upd_dt
        end as complications_risk_upd_dt,
        case when registry_metric_history.mstr_chrg_edit_rule_key = '85014'
            then row_number() over (
                partition by
                    stg_diabetes_patient.diabetes_reporting_month,
                    registry_data_info.pat_key
                order by
                    control_risk_upd_dt desc
            ) end as control_risk_rn,
        case when registry_metric_history.mstr_chrg_edit_rule_key = '78730'
            then row_number() over (
                partition by
                    stg_diabetes_patient.diabetes_reporting_month,
                    registry_data_info.pat_key
                order by
                    complications_risk_upd_dt desc
            ) end as complications_risk_rn
    from
        {{ ref('stg_diabetes_patient') }} as stg_diabetes_patient
        inner join {{ source('cdw', 'registry_data_info')}} as registry_data_info
            on stg_diabetes_patient.pat_key = registry_data_info.pat_key
        inner join {{ source('cdw', 'registry_metric_history') }} as registry_metric_history
            on registry_data_info.record_key = registry_metric_history.record_key
    where
        registry_metric_history.mstr_chrg_edit_rule_key in ('85014', --diabetes control risk score
                                                            '78730' --diabetes complications risk score
                                                            )
        and registry_metric_history.metric_last_upd_dt < stg_diabetes_patient.diabetes_reporting_month
        and registry_metric_history.metric_last_upd_dt
            >= stg_diabetes_patient.diabetes_reporting_month - interval('15 month')
)

select
    stg_diabetes_patient.diabetes_reporting_month,
    stg_diabetes_patient.report_card_4mo_pat_category,
    stg_diabetes_patient.pat_key,
    stg_diabetes_patient.patient_key,
    --Last edit on report point:
    cast(max(case
        when risk_score.control_risk_rn = 1
        then risk_score.risk_score
    end) as varchar(5)) as control_risk_score,
    --Last edit on report point:
    cast(max(case
        when risk_score.complications_risk_rn = 1
        then risk_score.risk_score
    end) as varchar(5)) as complications_risk_score
from
    {{ ref('stg_diabetes_patient') }} as stg_diabetes_patient
    left join risk_score
        on risk_score.pat_key = stg_diabetes_patient.pat_key
            and risk_score.diabetes_reporting_month = stg_diabetes_patient.diabetes_reporting_month
            and (risk_score.control_risk_rn = 1
                or risk_score.complications_risk_rn = 1)
group by
    stg_diabetes_patient.diabetes_reporting_month,
    stg_diabetes_patient.report_card_4mo_pat_category,
    stg_diabetes_patient.pat_key,
    stg_diabetes_patient.patient_key
