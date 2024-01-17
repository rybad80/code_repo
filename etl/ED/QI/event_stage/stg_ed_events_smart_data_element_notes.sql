select
  smart_data_element_all.visit_key,
  smart_data_element_all.csn,
  hno_info.note_id,
  hno_info.ip_note_type_c,
  max(
    case
        when hno_info.delete_instant_dttm is not null
          then 1
        else 0
    end
  ) as note_deleted_ind
from
  {{ref('smart_data_element_all')}} as smart_data_element_all
  inner join {{ source('cdw_analytics', 'fact_edqi') }} as fact_edqi
    on smart_data_element_all.visit_key = fact_edqi.visit_key
  inner join {{ source('clarity_ods', 'hno_info') }} as hno_info
    on smart_data_element_all.rec_id_char = hno_info.note_id
where
   lower(smart_data_element_all.context_name) = 'note'
group by
  smart_data_element_all.visit_key,
  smart_data_element_all.csn,
  hno_info.note_id,
  hno_info.ip_note_type_c
