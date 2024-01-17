select
    smart_data_element_data.hlv_id,
    stg_patient_ods.pat_id,
    date(smart_data_element_data.cur_value_datetime) as documented_date,
    null as received_date,
    1 as influenza_vaccine_ind,
    null as grouper_records_numeric_id--might need TO be NULL
from {{ source ('clarity_ods', 'smrtdta_elem_data') }} as smart_data_element_data
inner join {{ source ('clarity_ods', 'smrtdta_elem_value') }} as smart_data_element_value
    on smart_data_element_data.hlv_id = smart_data_element_value.hlv_id
inner join {{ source ('clarity_ods', 'clarity_concept') }} as clarity_concept
    on smart_data_element_data.element_id = clarity_concept.concept_id
inner join {{ref('stg_patient_ods')}} as stg_patient_ods
    on smart_data_element_data.pat_link_id = stg_patient_ods.pat_id
where
    clarity_concept.concept_id in ('EPIC#3151', 'MEDCIN#111742')
    and smart_data_element_value.smrtdta_elem_value not like '{}'
    and smart_data_element_value.smrtdta_elem_value not like   '{"response":""}'
    and (lower(smart_data_element_value.smrtdta_elem_value) like '%alreadygiven%'
        or lower(smart_data_element_value.smrtdta_elem_value) like '%pcp%')
