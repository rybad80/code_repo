select
    note_edit_metadata_history.note_visit_key,
    note_edit_metadata_history.note_key
from
    {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
where
    -- MH PROGRESS NOTE or MH CONSULT NOTE
    (note_edit_metadata_history.note_type_id in ('400040', '400041')
        or lower(note_edit_metadata_history.version_author_employee_title) like '%psychiatric tech%')
    and note_edit_metadata_history.encounter_date >= '2018-01-01'
