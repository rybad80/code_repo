select
    note_edit_metadata_history.pat_key,
    -- Try to use the following key as the above key will be deprecated:
    note_edit_metadata_history.patient_key
from
    {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.encounter_key = note_edit_metadata_history.encounter_key
    inner join {{ ref('ctis_registry') }} as ctis_registry
        on ctis_registry.patient_key = note_edit_metadata_history.patient_key
    inner join {{ ref('stg_note_text') }} as stg_note_text
        on stg_note_text.note_csn_id = note_edit_metadata_history.note_enc_id
where
    note_edit_metadata_history.last_edit_ind = 1
    and note_edit_metadata_history.note_type_id = 1 --'progress notes'
    and stg_encounter.encounter_type_id = 101 --'office visit'
    and regexp_like(lower(stg_note_text.note_text), '(external|nylon)[^\.]*(suture|closure)')
group by
    note_edit_metadata_history.pat_key,
    note_edit_metadata_history.patient_key
