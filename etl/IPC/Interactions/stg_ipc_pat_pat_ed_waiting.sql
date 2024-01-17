with ed_cohort as (
    select
        fact_edqi.pat_key,
        fact_edqi.visit_key,
        fact_edqi.arrive_ed_dt,
        coalesce(null, fact_edqi.admit_ed_dt, fact_edqi.triage_start_dt) as admit_ed_dt
    from
        {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
    where
        fact_edqi.arrive_ed_dt is not null
        and coalesce(null, fact_edqi.admit_ed_dt, fact_edqi.triage_start_dt) is not null
)

select
    index_patient.visit_key as action_key,
    1 as action_seq_num,
    index_patient.pat_key as index_patient_pat_key,
    index_patient.visit_key as index_patient_visit_key,
    match_patient.pat_key as match_patient_pat_key,
    match_patient.visit_key as match_patient_visit_key,
    date(index_patient.arrive_ed_dt) as event_date,
    index_patient.arrive_ed_dt as index_patient_start_date,
    index_patient.admit_ed_dt as index_patient_end_date,
    match_patient.arrive_ed_dt as matched_patient_start_date,
    match_patient.admit_ed_dt as matched_patient_end_date,
    null as location_index_bed,
    null as location_match_bed,
    null as location_room,
    'ed main' as location_department,
    null as location_department_group,
    'visit_key' as action_key_field,
    'derived' as action_seq_num_field,
    'same ed waiting area' as event_description
from
    ed_cohort as index_patient
    inner join ed_cohort as match_patient
        on date(match_patient.arrive_ed_dt) = date(index_patient.arrive_ed_dt)
        and date(match_patient.admit_ed_dt) = date(index_patient.admit_ed_dt)
where
    match_patient.arrive_ed_dt between index_patient.arrive_ed_dt and index_patient.admit_ed_dt
