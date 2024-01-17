select
    stg_note_visit_info.note_visit_key,
    smart_text.smart_text_key,
    stg_encounter.mrn,
    stg_encounter.encounter_date,
    smart_text.smart_text_id,
    smart_text.smart_text_name,
    stg_encounter.visit_key,
    stg_encounter.pat_key,
    stg_note_info.note_key,
    stg_note_visit_info.spec_time_loc_dttm as note_edit_date
from
    {{ref('stg_note_visit_info')}} as stg_note_visit_info
    inner join {{source('cdw', 'note_smart_text_id')}} as note_smart_text_id
        on note_smart_text_id.note_visit_key = stg_note_visit_info.note_visit_key
    inner join {{source('cdw', 'smart_text')}} as smart_text
        on smart_text.smart_text_key = note_smart_text_id.smart_text_key
    inner join {{ref('stg_note_info')}} as stg_note_info
        on stg_note_info.note_id = stg_note_visit_info.note_id
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = stg_note_info.pat_enc_csn_id
where
    lower(smart_text.smart_text_name) like '%ortho%'
    and lower(smart_text.smart_text_name) != 'apiorthoevra'
    and lower(smart_text.smart_text_name) not like '%contraceptive %'
    and lower(smart_text.smart_text_name) not like '% ed %'
    and lower(smart_text.smart_text_name) not like '%link2chop%'
    and lower(smart_text.smart_text_name) not like '%optime%'
    and lower(smart_text.smart_text_name) not like '%orthodontic%'
    and lower(smart_text.smart_text_name) not like '%orthotic%'
    and lower(smart_text.smart_text_name) not like '% ris %'
    and lower(smart_text.smart_text_name) not like 'ccn %'
    and lower(smart_text.smart_text_name) not like 'ed %'
    and lower(smart_text.smart_text_name) not like 'pla %'
    and lower(smart_text.smart_text_name) not like 'pt %'
    and lower(smart_text.smart_text_name) not like 'rehab %'
