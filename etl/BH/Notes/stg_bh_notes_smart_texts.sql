select
    note_edit_metadata_history.note_visit_key,
    note_edit_metadata_history.note_key,
    max(case when note_smarttext_ids.smarttexts_id in ('14152', '18771', '19046', '22316', '22317')
            then 1 else 0 end) as npv_ind,
    max(case when note_smarttext_ids.smarttexts_id in ('17330', '22318', '22319')
            then 1 else 0 end) as follow_ind
from
   {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
  inner join {{source('clarity_ods', 'note_smarttext_ids')}}  as note_smarttext_ids
        on note_smarttext_ids.note_id = note_edit_metadata_history.note_id
where
    note_smarttext_ids.smarttexts_id in
    ('14152',
        '18771',
        '19046',
        '22316',
        '22317',
        '17330',
        '22318',
        '22319'
    )
    and note_edit_metadata_history.encounter_date >= '2018-01-01'
group by
    note_edit_metadata_history.note_visit_key,
    note_edit_metadata_history.note_key
