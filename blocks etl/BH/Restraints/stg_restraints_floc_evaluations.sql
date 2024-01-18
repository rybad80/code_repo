--identify .RESTRAINT components in notes
--"run" with following sequence to prevent note separation
select
    visit_key,
    note_key,
    note_concat,
    seq_num,
    --beginning of the .RESTRAINT template
    --template is intended for violent restraints, but author might alter for non-violent restraint use
    case when note_concat like '%-violent restraints were initiated%'
        then 40071712 else 40071755 end as group_id,
    regexp_instr(
        note_concat,
        'violent restraints were initiated'
    ) as group_index,
    regexp_extract(
        note_concat,
        'face-to-face evaluation of the patient'
        || '(\D+)?(\d+/\d+/\d+)?(\D*)?(\d+/\d+/\d+)?(\D*)?\d{1,2}(:)?(\s+)?(\d+)?(\s*)?(am|pm|AM|PM)?'
    ) as evaluation_text,
    --multiple evaluations can be present in the same note; typically in ED Provider notes
    --identify the index where each template component begins in case multiple evaluations are present
    regexp_instr(
        note_concat,
        'face-to-face evaluation'
    ) + 8000 * (seq_num - 1) as evaluation_index, --based on character limit of concatenated notes
    --date of FLOC evaluation
    date(
        regexp_extract(
            evaluation_text,
            '\d+/\d+/\d{2,4}'
        )
    ) as floc_date,
    service_date,
    --extract timestamp from note
    lower(
        regexp_replace(
            evaluation_text,
            'face-to-face evaluation of the patient(\D+)?(\d+/\d+/\d+)?(\D*)?(\d+/\d+/\d+)?(\D*)?',
            ''
        )
    ) as floc_time,
    case when note_concat like '%reasons for restraint event initiation%'
        then 1 else 0 end as reasons_ind,
    regexp_instr(
        note_concat,
        'reasons for restraint event initiation'
    ) + 8000 * (seq_num - 1) as reasons_index,
    case when note_concat like '%response to application of restraint%'
        then 1 else 0 end as pat_response_ind,
    regexp_instr(
        note_concat,
        'response to application of restraint'
    ) + 8000 * (seq_num - 1) as pat_response_index,
    --Attending of record: not case-sensitive
    case when note_concat like '%ttending of record%'
        then 1 else 0 end as attending_notified_ind,
    regexp_instr(
        note_concat,
        -- 'ttending of record'
        'ttending of record about this restraint event'
    ) + 8000 * (seq_num - 1) as attending_notified_index,
    case when note_concat like '%summary of restraint%'
        then 1 else 0 end as summary_ind,
    regexp_instr(
        note_concat,
        'summary of restraint'
    ) + 8000 * (seq_num - 1) as summary_index
from
    {{ ref('stg_restraints_provider_notes') }}
