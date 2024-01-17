select
    note_edit_metadata_history.visit_key,
    note_edit_metadata_history.version_author_name,
    note_edit_metadata_history.encounter_date,
    note_edit_metadata_history.note_type
from
    {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
    left join {{ source('cdw', 'note_text') }} as note_text
        on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_frontier_program_providers
        on lower(note_edit_metadata_history.version_author_name)
            = lower(lookup_frontier_program_providers.provider_name)
            and lookup_frontier_program_providers.program = 'cva'
where
    year(add_months(note_edit_metadata_history.encounter_date, 6)) >= 2019
    and last_edit_ind = 1
    and (lower(note_text) like '%comprehensive vascular anomalies%'
        or lower(note_text) like '%cvap%')
