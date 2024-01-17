with all_ktv as (--region all kt/v values from flowsheets.
    select
        flowsheet_all.pat_key,
        flowsheet_all.mrn,
        flowsheet_all.patient_name,
        flowsheet_all.dob,
        case when flowsheet_id = '13905' then 'HD' else 'PD' end as dialysis_type,
        max(case when stg_encounter.age_years >= 18 and dialysis_type = 'PD'
        then null else round(meas_val_num, 2) end) as month_ktv,
        month(flowsheet_all.encounter_date) || '-' || year(flowsheet_all.encounter_date) as month_year,
        max(year(flowsheet_all.encounter_date)) as calendar_year,
        submission_year,
        division,
        metric_name,
        question_number,
        start_date,
        end_date,
        metric_id
    from {{ ref('flowsheet_all')}} as flowsheet_all
    inner join {{ ref('stg_usnews_nephrology_dialysis_calendar')}} as stg_usnews_nephrology_dialysis_calendar
            on flowsheet_all.pat_key = stg_usnews_nephrology_dialysis_calendar.pat_key
                and flowsheet_all.encounter_date >= maintenance_dialysis_start_date
                and question_number = 'g23'
        inner join {{ ref('stg_encounter')}} as stg_encounter
            on flowsheet_all.visit_key = stg_encounter.visit_key
    where
        flowsheet_id in (
        '400709022', -- Total KT/V
        '13905', -- spKt/V (single pool Kt/V for 3 days of hemodialysis)
        '400709020') --KT/V
        and maintenance_dialysis_start_date <= flowsheet_all.encounter_date
    group by
        flowsheet_all.pat_key,
        flowsheet_all.mrn,
        flowsheet_all.patient_name,
        flowsheet_all.dob,
        dialysis_type,
        month_year,
        submission_year,
        division,
        metric_name,
        question_number,
        start_date,
        end_date,
        metric_id,
        flowsheet_id
),

stage as (
    select
        pat_key,
        mrn,
        patient_name,
        dob,
        dialysis_type,
        max(month_ktv) as max_month_ktv, -- need max because flowsheet group by in cte can cause two values
        month_year,
        {{
        dbt_utils.surrogate_key([
            'pat_key',
            'month_year'
            ])
        }} as primary_key,
        submission_year,
        division,
        question_number,
        case when dialysis_type = 'HD' and submission_year = calendar_year + 1 then 'g23a2'
            when dialysis_type = 'HD' and submission_year = calendar_year + 2 then 'g23a1'
            when dialysis_type = 'PD' and submission_year = calendar_year + 1 then 'g23b2'
            when dialysis_type = 'PD' and submission_year = calendar_year + 2 then 'g23b1'
            end as metric_id,
        case when dialysis_type = 'PD' and max_month_ktv >= 1.8 then 1 else 0 end as pd_threshold,
        case when dialysis_type = 'HD' and max_month_ktv >= 1.2 then 1 else 0 end as hd_threshold
    from all_ktv
    group by
        pat_key,
        mrn,
        dob,
        patient_name,
        dialysis_type,
        month_year,
        calendar_year,
        submission_year,
        division,
        question_number
)

select
    pat_key,
    patient_name,
    mrn,
    dob,
    month_year,
    dialysis_type,
    max_month_ktv,
    submission_year,
    division,
    question_number,
    metric_id,
    primary_key,
    case when hd_threshold = 1  then primary_key
        when pd_threshold = 1 then primary_key
        else null end as num,
    primary_key as denom
from
    stage
where metric_id is not null
