select
    note_visit_key,
    visit_key,
    pat_key,
    note_key
from
    {{ref('stg_ortho_notes_by_specialty')}}
union
select
    note_visit_key,
    visit_key,
    pat_key,
    note_key
from
    {{ref('stg_ortho_notes_by_service')}}
union
select
    note_visit_key,
    visit_key,
    pat_key,
    note_key
from
    {{ref('stg_ortho_smart_text_notes')}}
