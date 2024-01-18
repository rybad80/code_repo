select
    ctis_registry.pat_key,
    ctis_registry.mrn,
    patient_staff_note.acct_note_dt as noted_date,
    patient_staff_note.active_ind as fyi_flag_active_ind,
    patient_staff_note.note_key,
    dim_bpa_locator_trigger.bpa_locator_trig_nm as bpa_trigger_name,
    dim_bpa_locator_trigger.bpa_locator_trig_id as bpa_trigger_id,
    -- Replace all non-ascii characters with a space:
    regexp_replace(stg_note_text.note_text, '[\x80-\xFF]', ' ', 1, 0, 'u') as note_text,
    stg_note_text.line_number as seq_num
from
    {{ ref('ctis_registry') }} as ctis_registry
    inner join {{ source('cdw', 'patient_staff_note') }} as patient_staff_note
        on patient_staff_note.pat_key = ctis_registry.pat_key
    inner join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
        on note_edit_metadata_history.note_key = patient_staff_note.note_key
    inner join {{ source('cdw', 'dim_bpa_locator_trigger') }} as dim_bpa_locator_trigger
        on dim_bpa_locator_trigger.dim_bpa_locator_trig_key = patient_staff_note.dim_bpa_locator_trig_key
    inner join {{ ref('stg_note_text') }} as stg_note_text
        on stg_note_text.note_csn_id = note_edit_metadata_history.note_enc_id
where
    note_edit_metadata_history.last_edit_ind = 1
