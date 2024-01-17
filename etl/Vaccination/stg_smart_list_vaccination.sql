select
    smart_data_element_data.hlv_id,
    stg_patient_ods.pat_id,
    null as grouper_records_numeric_id,
    date(smart_data_element_data.cur_value_datetime) as documented_date,
    null as received_date,
    1 as influenza_vaccine_ind
from {{ source ('clarity_ods', 'hno_info') }} as hno_info
    inner join {{ref('stg_patient_ods')}} as stg_patient_ods
        on hno_info.pat_id = stg_patient_ods.pat_id
    inner join {{ source ('clarity_ods', 'note_smartlist_ids') }} as note_smart_list_id
        on hno_info.note_id = note_smart_list_id.note_id
    inner join {{ source ('cdw', 'smart_list') }} as smart_list
        on note_smart_list_id.smartlists_id = smart_list.list_id
    inner join {{ source ('clarity_ods', 'smrtdta_elem_data') }} as smart_data_element_data
        on hno_info.pat_id = smart_data_element_data.pat_link_id
    inner join {{ source ('clarity_ods', 'smrtdta_elem_value') }} as smart_data_element_value
        on smart_data_element_value.hlv_id = smart_data_element_data.hlv_id
where smart_list.list_id = 14989-- pulm imm
    and lower(smart_data_element_value.smrtdta_elem_value) = 'influenza current'
