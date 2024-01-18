with fever_temp as (
    select distinct
        encounter_ed.visit_key,
        1 as  fever_temp_ind
    from
        {{ ref('encounter_ed') }} as encounter_ed
        inner join {{ ref('flowsheet_all') }} as flowsheet_all on encounter_ed.visit_key = flowsheet_all.visit_key
    where
        flowsheet_id = 6 --'Temp'
        and flowsheet_all.meas_val_num is not null
        and flowsheet_all.recorded_date <= encounter_ed.ed_discharge_date
        and flowsheet_all.meas_val_num >= 100.4
),

fever_complaint as (

    select
        encounter_ed.visit_key,
        1 as fever_complaint,
        max(case when seq_num = 1 then 1 else 0 end ) as chief_complaint
    from
        {{ ref('encounter_ed') }} as encounter_ed
        inner join
            {{ source('cdw', 'visit_reason') }} as visit_reason on encounter_ed.visit_key = visit_reason.visit_key
        inner join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
            on master_reason_for_visit.rsn_key = visit_reason.rsn_key
    where
        upper(rsn_nm) like '%FEVER%'
    group by
        encounter_ed.visit_key
)

select
    encounter_ed.visit_key,
    encounter_ed.pat_key,
    'FEVER_DISCHARGE' as cohort,
    null as subcohort
from
    {{ ref('encounter_ed') }} as encounter_ed
    inner join fever_temp                 on fever_temp.visit_key = encounter_ed.visit_key
    inner join fever_complaint            on fever_complaint.visit_key = encounter_ed.visit_key
where
    year(encounter_ed.encounter_date) >= year(current_date) - 5
    and encounter_ed.acuity_esi in ('4 Urgent', '5 Non-Urgent')
    and encounter_ed.age_days >= 60
    and encounter_ed.complex_chronic_condition_ind = 0
    and encounter_ed.inpatient_ind = 0
    and fever_temp.fever_temp_ind = 1
    and fever_complaint.fever_complaint = 1
