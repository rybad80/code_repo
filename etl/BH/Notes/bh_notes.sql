select
    stg_bh_notes.note_visit_key,
    stg_bh_notes.note_key,
    note_edit_metadata_history.pat_key,
    note_edit_metadata_history.visit_key,
    note_edit_metadata_history.patient_name,
    note_edit_metadata_history.mrn,
    note_edit_metadata_history.csn,
    note_edit_metadata_history.encounter_date,
    note_edit_metadata_history.age_years,
    note_edit_metadata_history.provider_name,
    note_edit_metadata_history.provider_id,
    note_edit_metadata_history.department_name,
    note_edit_metadata_history.note_type,
    note_edit_metadata_history.note_type_id,
    note_edit_metadata_history.note_id,
    note_edit_metadata_history.version_author_provider_name,
    note_edit_metadata_history.version_author_provider_title,
    note_edit_metadata_history.version_author_employee_title,
    note_edit_metadata_history.final_author_name,
    note_edit_metadata_history.final_author_title,
    note_edit_metadata_history.final_author_prov_key,
    note_edit_metadata_history.version_author_name,
    note_edit_metadata_history.version_author_title,
    note_edit_metadata_history.version_author_service_name,
    note_edit_metadata_history.version_author_service_id,
    note_edit_metadata_history.edit_seq_number,
    note_edit_metadata_history.service_date,
    note_edit_metadata_history.note_status,
    note_edit_metadata_history.note_status_id,
    note_edit_metadata_history.last_edit_ind,
    case
        when note_edit_metadata_history.note_status_id in ('1', '8', '9')
        then 1 else 0 end as open_note_ind,
    case
        when open_note_ind = 0
        then note_edit_metadata_history.note_entry_date end as signed_dt,
    note_edit_metadata_history.block_last_update_date,
    coalesce(stg_bh_notes_npv_follow.npv_ind, 0) as npv_ind,
    coalesce(stg_bh_notes_npv_follow.follow_ind, 0) as follow_ind,
    stg_bh_notes_time_spent.time_spent_with_patient_mins,
    stg_bh_notes_time_spent.time_consulting_team_mins
from
    {{ref('stg_bh_notes')}} as stg_bh_notes
    inner join {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
        on note_edit_metadata_history.note_visit_key = stg_bh_notes.note_visit_key
    left join {{ref('stg_bh_notes_npv_follow')}} as stg_bh_notes_npv_follow
        on stg_bh_notes_npv_follow.note_key = stg_bh_notes.note_key
    left join {{ref('stg_bh_notes_time_spent')}} as stg_bh_notes_time_spent
        on stg_bh_notes_time_spent.note_key = stg_bh_notes.note_key
