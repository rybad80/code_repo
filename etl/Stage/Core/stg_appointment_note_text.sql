{{ config(meta = {
    'critical': true
}) }}

select
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    -- group_concat() has a limit of 4,000 characters. So, very long notes will be truncated and
    -- there may be artifacts left over such as '!!!dele' in the output string.
    -- This existed in only two rows on 9/13/2023: visit_key = 90298145, 85345654:
    trim(
        regexp_replace(
            group_concat(
                '!!!delete_from_sql_start_'
                || lpad(pat_enc_appt_notes.line, 10, '0')
                || '_delete_from_sql_end!!!'
                || pat_enc_appt_notes.appt_note, ' '
            ),
            '\!!!delete_from_sql_start_\d+\_delete_from_sql_end!!!',
            '  '
        )
    ) as appointment_note_text
from
    {{ ref('stg_encounter') }} as stg_encounter
    inner join {{ source('clarity_ods', 'pat_enc_appt_notes') }} as pat_enc_appt_notes
        on stg_encounter.csn = pat_enc_appt_notes.pat_enc_csn_id
group by
    stg_encounter.encounter_key,
    stg_encounter.visit_key
