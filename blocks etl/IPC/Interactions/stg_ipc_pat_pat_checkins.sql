with checkins as (
    select
        visit_ed_event.visit_key,
        visit_ed_event.pat_key,
        encounter_all.dept_key,
        encounter_all.department_name,
        min(visit_ed_event.visit_ed_event_key) as visit_ed_event_key,
        min(case when master_event_type.event_id = 57021 then visit_ed_event.event_dt end) as start_date,
        max(case when master_event_type.event_id = 600 then visit_ed_event.event_dt end) as end_date
    from
        {{source('cdw', 'visit_ed_event')}} as visit_ed_event
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
            on master_event_type.event_type_key = visit_ed_event.event_type_key
        inner join {{ref('encounter_all')}} as encounter_all
            on encounter_all.visit_key = visit_ed_event.visit_key
    where
        master_event_type.event_id in (57021, 600) /* checkin start, checkin complete */
    group by
        visit_ed_event.visit_key,
        visit_ed_event.pat_key,
        encounter_all.dept_key,
        encounter_all.department_name
)

select
    index_patient.visit_ed_event_key as action_key,
    1 as action_seq_num,
    index_patient.pat_key as index_patient_pat_key,
    index_patient.visit_key as index_patient_visit_key,
    match_patient.pat_key as match_patient_pat_key,
    match_patient.visit_key as match_patient_visit_key,
    date(index_patient.start_date) as event_date,
    index_patient.start_date as index_patient_start_date,
    index_patient.end_date as index_patient_end_date,
    match_patient.start_date as matched_patient_start_date,
    match_patient.end_date as matched_patient_end_date,
    null as location_index_bed,
    null as location_match_bed,
    null as location_room,
    index_patient.department_name as location_department,
    null as location_department_group,
    'visit_ed_event_key' as action_key_field,
    'derived' as action_seq_num_field,
    'same checkin area' as event_description
from
    checkins as index_patient
    inner join checkins as match_patient
        on match_patient.dept_key = index_patient.dept_key
        and date(match_patient.start_date) = date(index_patient.start_date)
        and date(match_patient.end_date) = date(index_patient.end_date)
where
    index_patient.pat_key != match_patient.pat_key
    and index_patient.visit_key != match_patient.visit_key
    and match_patient.start_date is not null
    and index_patient.end_date is not null
    and match_patient.end_date is not null
    and match_patient.start_date between index_patient.start_date and index_patient.end_date
