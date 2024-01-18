select
    index_patient.visit_event_key as action_key,
    index_patient.all_bed_order as action_seq_num,
    index_patient.pat_key as index_patient_pat_key,
    index_patient.visit_key as index_patient_visit_key,
    match_patient.pat_key as match_patient_pat_key,
    match_patient.visit_key as match_patient_visit_key,
    index_patient.encounter_date as event_date,
    index_patient.enter_date as index_patient_start_date,
    index_patient.exit_date as index_patient_end_date,
    match_patient.enter_date as matched_patient_start_date,
    match_patient.exit_date_or_current_date as matched_patient_end_date,
    index_patient.bed_name as location_index_bed,
    match_patient.bed_name as location_match_bed,
    index_patient.room_name as location_room,
    index_patient.department_name as location_department,
    index_patient.department_group_name as location_department_group,
    'visit_event_key' as action_key_field,
    'all_bed_order' as action_seq_num_field,
    'same inpatient room' as event_description
from
    {{ref('adt_bed')}} as index_patient
    inner join {{ref('adt_bed')}} as match_patient
        on index_patient.room_key = match_patient.room_key
where
    match_patient.enter_date between index_patient.enter_date and index_patient.exit_date
    and index_patient.pat_key != match_patient.pat_key
    and index_patient.room_key != -1
    and index_patient.enter_date > '2015-01-01'
    and index_patient.bed_key != match_patient.bed_key
