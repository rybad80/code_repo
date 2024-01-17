select
     note_text.note_visit_key,
     note_text.note_text,
     note_text.seq_num
from
     {{source('cdw', 'note_text')}} as note_text
where
     lower(note_text.note_text) like '%bayley%scales%'
