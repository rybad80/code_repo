select
    note_edit_metadata_history.note_visit_key,
    note_edit_metadata_history.patient_name,
    note_edit_metadata_history.mrn,
    note_edit_metadata_history.csn,
    note_edit_metadata_history.encounter_date,
    note_edit_metadata_history.sex,
    stg_patient.race_ethnicity,
    note_edit_metadata_history.age_years,
    note_edit_metadata_history.provider_name,
    note_edit_metadata_history.provider_id,
    note_edit_metadata_history.department_name,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    note_edit_metadata_history.note_type,
    note_edit_metadata_history.note_type_id,
    note_edit_metadata_history.note_id,
    note_edit_metadata_history.version_author_name,
    note_edit_metadata_history.service_date,
    case
        when lower(note_edit_metadata_history.version_author_title) in ('md', 'do') then 'Physician'
        when lower(note_edit_metadata_history.version_author_title) in ('crnp', 'pa', 'pa-c') then 'APP'
        when lower(note_edit_metadata_history.version_author_title) like 'scribe%' then 'Scribe'
        else 'Other'
    end as version_author_category,
    dim_provider_type.prov_type_nm as version_provider_type,
    note_edit_metadata_history.version_author_service_name,
    note_edit_metadata_history.version_author_service_id,
    note_edit_metadata_history.edit_seq_number,
    row_number() over(
        partition by note_edit_metadata_history.note_key order by note_edit_metadata_history.edit_seq_number desc
    ) as edit_seq_num_latest,
    note_edit_metadata_history.note_entry_date,
    year(add_months(note_edit_metadata_history.encounter_date, 6)) as fiscal_year,
    year(note_edit_metadata_history.encounter_date) as calendar_year,
    date_trunc('month', note_edit_metadata_history.encounter_date) as calendar_month,
    note_edit_metadata_history.note_key,
    note_edit_metadata_history.pat_key,
    note_edit_metadata_history.visit_key,
    note_edit_metadata_history.prov_key,
    note_edit_metadata_history.version_author_emp_key
from
    {{ref('stg_ortho_notes_combined')}} as stg_ortho_notes_combined
    inner join {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
        on note_edit_metadata_history.note_visit_key = stg_ortho_notes_combined.note_visit_key
    inner join {{ref('stg_note_visit_info')}} as stg_note_visit_info
        on stg_note_visit_info.contact_serial_num = note_edit_metadata_history.note_enc_id
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = note_edit_metadata_history.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = note_edit_metadata_history.pat_key
    left join {{source('cdw', 'dim_provider_type')}} as dim_provider_type
        on dim_provider_type.prov_type_id = stg_note_visit_info.author_prvd_type_id
where
    note_edit_metadata_history.note_deleted_ind = 0
