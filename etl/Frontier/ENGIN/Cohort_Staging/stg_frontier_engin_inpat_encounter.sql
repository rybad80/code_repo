-- inpatient consult encounters
select
    note_edit_metadata_history.visit_key,
    note_edit_metadata_history.encounter_date,
    note_edit_metadata_history.pat_key
from {{ ref('note_edit_metadata_history') }}  as note_edit_metadata_history
inner join {{source('cdw', 'note_text')}} as note_text
    on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_fp_providers
    on lower(note_edit_metadata_history.version_author_name) = lookup_fp_providers.provider_name
    and lookup_fp_providers.program = 'engin'
    and lookup_fp_providers.provider_type = 'genetic counselor' -- written by the engin genetic counselors
    and lookup_fp_providers.active_ind = 1
where
    lower(note_type) = 'consult note'
    and regexp_like(lower(note_text), '\bengin\b') -- consult note contains the word of engin
    and note_edit_metadata_history.edit_seq_number = 1 -- first version of the note (created)
    and year(add_months(note_edit_metadata_history.service_date, 6)) >= 2020
    and note_edit_metadata_history.visit_key is not null
group by
    note_edit_metadata_history.visit_key,
    note_edit_metadata_history.encounter_date,
    note_edit_metadata_history.pat_key
