select
    stg_ipc_pat_pat_adt_bed.action_key,
    stg_ipc_pat_pat_adt_bed.action_seq_num,
    stg_ipc_pat_pat_adt_bed.index_patient_pat_key,
    stg_ipc_pat_pat_adt_bed.index_patient_visit_key,
    stg_ipc_pat_pat_adt_bed.match_patient_pat_key,
    stg_ipc_pat_pat_adt_bed.match_patient_visit_key,
    stg_ipc_pat_pat_adt_bed.event_date,
    stg_ipc_pat_pat_adt_bed.index_patient_start_date,
    stg_ipc_pat_pat_adt_bed.index_patient_end_date,
    stg_ipc_pat_pat_adt_bed.matched_patient_start_date,
    stg_ipc_pat_pat_adt_bed.matched_patient_end_date,
    stg_ipc_pat_pat_adt_bed.location_index_bed,
    stg_ipc_pat_pat_adt_bed.location_match_bed,
    stg_ipc_pat_pat_adt_bed.location_room,
    stg_ipc_pat_pat_adt_bed.location_department,
    stg_ipc_pat_pat_adt_bed.location_department_group,
    stg_ipc_pat_pat_adt_bed.action_key_field,
    stg_ipc_pat_pat_adt_bed.action_seq_num_field,
    stg_ipc_pat_pat_adt_bed.event_description
from
    {{ref('stg_ipc_pat_pat_adt_bed')}} as stg_ipc_pat_pat_adt_bed
union all
select
    stg_ipc_pat_pat_tracker_matches.action_key,
    stg_ipc_pat_pat_tracker_matches.action_seq_num,
    stg_ipc_pat_pat_tracker_matches.index_patient_pat_key,
    stg_ipc_pat_pat_tracker_matches.index_patient_visit_key,
    stg_ipc_pat_pat_tracker_matches.match_patient_pat_key,
    stg_ipc_pat_pat_tracker_matches.match_patient_visit_key,
    stg_ipc_pat_pat_tracker_matches.event_date,
    stg_ipc_pat_pat_tracker_matches.index_patient_start_date,
    stg_ipc_pat_pat_tracker_matches.index_patient_end_date,
    stg_ipc_pat_pat_tracker_matches.matched_patient_start_date,
    stg_ipc_pat_pat_tracker_matches.matched_patient_end_date,
    stg_ipc_pat_pat_tracker_matches.location_index_bed,
    stg_ipc_pat_pat_tracker_matches.location_match_bed,
    stg_ipc_pat_pat_tracker_matches.location_room,
    stg_ipc_pat_pat_tracker_matches.location_department,
    stg_ipc_pat_pat_tracker_matches.location_department_group,
    stg_ipc_pat_pat_tracker_matches.action_key_field,
    stg_ipc_pat_pat_tracker_matches.action_seq_num_field,
    stg_ipc_pat_pat_tracker_matches.event_description
from
    {{ref('stg_ipc_pat_pat_tracker_matches')}} as stg_ipc_pat_pat_tracker_matches
union all
select
    stg_ipc_pat_pat_checkins.action_key,
    stg_ipc_pat_pat_checkins.action_seq_num,
    stg_ipc_pat_pat_checkins.index_patient_pat_key,
    stg_ipc_pat_pat_checkins.index_patient_visit_key,
    stg_ipc_pat_pat_checkins.match_patient_pat_key,
    stg_ipc_pat_pat_checkins.match_patient_visit_key,
    stg_ipc_pat_pat_checkins.event_date,
    stg_ipc_pat_pat_checkins.index_patient_start_date,
    stg_ipc_pat_pat_checkins.index_patient_end_date,
    stg_ipc_pat_pat_checkins.matched_patient_start_date,
    stg_ipc_pat_pat_checkins.matched_patient_end_date,
    stg_ipc_pat_pat_checkins.location_index_bed,
    stg_ipc_pat_pat_checkins.location_match_bed,
    stg_ipc_pat_pat_checkins.location_room,
    stg_ipc_pat_pat_checkins.location_department,
    stg_ipc_pat_pat_checkins.location_department_group,
    stg_ipc_pat_pat_checkins.action_key_field,
    stg_ipc_pat_pat_checkins.action_seq_num_field,
    stg_ipc_pat_pat_checkins.event_description
from
    {{ref('stg_ipc_pat_pat_checkins')}} as stg_ipc_pat_pat_checkins
union all
select
    stg_ipc_pat_pat_ed_waiting.action_key,
    stg_ipc_pat_pat_ed_waiting.action_seq_num,
    stg_ipc_pat_pat_ed_waiting.index_patient_pat_key,
    stg_ipc_pat_pat_ed_waiting.index_patient_visit_key,
    stg_ipc_pat_pat_ed_waiting.match_patient_pat_key,
    stg_ipc_pat_pat_ed_waiting.match_patient_visit_key,
    stg_ipc_pat_pat_ed_waiting.event_date,
    stg_ipc_pat_pat_ed_waiting.index_patient_start_date,
    stg_ipc_pat_pat_ed_waiting.index_patient_end_date,
    stg_ipc_pat_pat_ed_waiting.matched_patient_start_date,
    stg_ipc_pat_pat_ed_waiting.matched_patient_end_date,
    stg_ipc_pat_pat_ed_waiting.location_index_bed,
    stg_ipc_pat_pat_ed_waiting.location_match_bed,
    stg_ipc_pat_pat_ed_waiting.location_room,
    stg_ipc_pat_pat_ed_waiting.location_department,
    stg_ipc_pat_pat_ed_waiting.location_department_group,
    stg_ipc_pat_pat_ed_waiting.action_key_field,
    stg_ipc_pat_pat_ed_waiting.action_seq_num_field,
    stg_ipc_pat_pat_ed_waiting.event_description
from
    {{ref('stg_ipc_pat_pat_ed_waiting')}} as stg_ipc_pat_pat_ed_waiting