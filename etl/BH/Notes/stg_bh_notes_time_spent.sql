select
    smart_data_element_all.note_key,
    max(case
            when smart_data_element_all.concept_id = 'CHOP#5322'
                then cast(trim(smart_data_element_all.element_value) as int) end)
    as time_spent_with_patient_mins,
    max(case
            when smart_data_element_all.concept_id = 'CHOPBH#234'
                then cast(trim(smart_data_element_all.element_value) as int) end)
    as time_consulting_team_mins
from
    {{ref('smart_data_element_all')}} as smart_data_element_all
where
    smart_data_element_all.concept_id in ('CHOP#5322', 'CHOPBH#234')
    and smart_data_element_all.encounter_date >= '2018-01-01'
    and smart_data_element_all.element_value not like '0.0-%'
group by
    smart_data_element_all.note_key
