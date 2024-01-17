select
    stg_pcoti_notes.pat_key,
    stg_pcoti_notes.visit_key,
    case
        when stg_pcoti_notes.note_type = 'IP CAT INITIATION NOTE' then 'Note - CAT Initiation'
        when stg_pcoti_notes.note_type = 'IP CAT EVALUATION NOTE' then 'Note - CAT Evaluation'
        when stg_pcoti_notes.note_type = 'IP CCOT NOTE' then 'Note - CCOT'
        when stg_pcoti_notes.note_type = 'IP CCOT RN/RT NOTE' then 'Note - CCOT RN/RT'
        when stg_pcoti_notes.note_type = 'IP CAT FOLLOWUP ASSESSMENT NOTE' then 'Note - CAT Followup Assessment'
    end as event_type_name,
    case
        when stg_pcoti_notes.note_type = 'IP CAT INITIATION NOTE' then 'NOTE_CAT_INIT'
        when stg_pcoti_notes.note_type = 'IP CAT EVALUATION NOTE' then 'NOTE_CAT_EVAL'
        when stg_pcoti_notes.note_type = 'IP CCOT NOTE' then 'NOTE_CCOT'
        when stg_pcoti_notes.note_type = 'IP CCOT RN/RT NOTE' then 'NOTE_CCOT_RN_RT'
        when stg_pcoti_notes.note_type = 'IP CAT FOLLOWUP ASSESSMENT NOTE' then 'NOTE_CAT_FOLLOWUP'
    end as event_type_abbrev,
    stg_pcoti_notes.note_join_date as event_start_date,
    null as event_end_date
from
    {{ ref('stg_pcoti_notes') }} as stg_pcoti_notes
where
    coalesce(
        stg_pcoti_notes.note_signed_date,
        stg_pcoti_notes.note_create_date
    ) >= '2017-01-01'
    and stg_pcoti_notes.note_signed_ind = 1
