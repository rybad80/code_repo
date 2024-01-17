select
    visit_ed_event.visit_key,
    visit_ed_event.event_dt,
    master_event_type.event_desc,
    master_event_type.event_id,
    case when master_event_type.event_id = 1120000020
                and lower(stg_note_text.note_text) like '%donor%cross%clamp%'
            then 112700007
            else master_event_type.event_id end as event_id_new
from
    {{source('cdw', 'visit_ed_event')}} as visit_ed_event
    inner join {{source('cdw', 'master_event_type')}} as master_event_type
        on visit_ed_event.event_type_key = master_event_type.event_type_key
    left join {{source('clarity_ods', 'ed_iev_event_info')}} as iev
        on iev.event_id = visit_ed_event.pat_event_id
            and iev.line = visit_ed_event.seq_num
    left join {{ref('stg_note_text')}} as stg_note_text
        on stg_note_text.note_id = iev.event_note_id
where
    (master_event_type.event_id in (112700007, 112700008, 1120000034, 1120000035)
    and event_stat is null)
    or (master_event_type.event_id = 1120000020
        and lower(stg_note_text.note_text) like '%donor%cross%clamp%')
