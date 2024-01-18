select
    index_patient.pat_loc_event_key as action_key,
    1 as action_seq_num,
    index_patient.pat_key as index_patient_pat_key,
    index_patient.visit_key as index_patient_visit_key,
    match_patient.pat_key as match_patient_pat_key,
    match_patient.visit_key as match_patient_visit_key,
    index_patient.encounter_date as event_date,
    index_patient.start_date as index_patient_start_date,
    index_patient.end_date as index_patient_end_date,
    match_patient.start_date as matched_patient_start_date,
    match_patient.end_date as matched_patient_end_date,
    null as location_index_bed,
    null as location_match_bed,
    index_patient.waiting_room_name as location_room,
    stg_encounter.department_name as location_department,
    null as location_department_group,
    'pat_loc_event_key' as action_key_field,
    'derived' as action_seq_num_field,
    case
        when lower(index_patient.waiting_room_name) like '%wait%' then 'same waiting room'
        when lower(index_patient.waiting_room_name) like '%exam%' then 'same exam room'
        when lower(index_patient.waiting_room_name) like '% ex %' then 'same exam room'
        when lower(index_patient.waiting_room_name) like '%operating room%' then 'same operating room'
        when lower(index_patient.waiting_room_name) like '%room%' then 'same exam room'
        when lower(index_patient.waiting_room_name) like '%bed%' then 'same bed'
        when lower(index_patient.waiting_room_name) like '%vitals%' then 'same vitals'
    end as event_description
from
    {{ref('stg_ipc_pat_pat_tracker_rooms')}} as index_patient
    inner join {{ref('stg_ipc_pat_pat_tracker_rooms')}} as match_patient
        on match_patient.pat_loc_finder_key = index_patient.pat_loc_finder_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = index_patient.visit_key
where
    match_patient.start_date between index_patient.start_date and index_patient.end_date
    and index_patient.end_date != match_patient.start_date
    and index_patient.visit_key != match_patient.visit_key
    and index_patient.pat_key != match_patient.pat_key
    and {{ limit_dates_for_dev(ref_date = 'index_patient.encounter_date') }}
