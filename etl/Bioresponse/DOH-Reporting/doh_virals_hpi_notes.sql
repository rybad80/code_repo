/* HPI notes brought in for IPC team to evaluate presenting sx, this is not a reporting field
Granuarity is at the note level, with multiple lines per note, and gets cleaned up
in the R dashbard*/

/* Query pulling HPI notes specific to encounters with infectious episodes
that get reported to the DOH
Brought in so IPC can scan through to identify any sx that weren't caught
in flowsheet
Final granularity should be one row per encounter*/

select
    note_edit_metadata_history.encounter_key,
    note_edit_metadata_history.note_enc_id,
    stg_doh_virals_cohort.encounter_episode_key,
    hno_note_text.line,
    hno_note_text.note_text
from
    (
        select
            stg_doh_virals_cohort.encounter_key,
            stg_doh_virals_cohort.encounter_episode_key
        from {{ ref('stg_doh_virals_cohort') }} as stg_doh_virals_cohort
        where stg_doh_virals_cohort.order_of_tests = 1
        group by
            stg_doh_virals_cohort.encounter_key,
            stg_doh_virals_cohort.encounter_episode_key
    ) as stg_doh_virals_cohort
    inner join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
        using (encounter_key)
    inner join {{ source('clarity_ods', 'hno_note_text') }} as hno_note_text
        on note_edit_metadata_history.note_enc_id = hno_note_text.note_csn_id
where
    note_edit_metadata_history.note_type_id = 19 -- ED Provider Note
    and note_edit_metadata_history.last_edit_ind = 1
