select
    smart_data_element_all.note_key,
    max(
        case
            when
                smart_data_element_all.concept_id in (
                    'CHOPBH#606',
                    'CHOPBH#607',
                    'CHOPBH#608',
                    'CHOPBH#609',
                    'CHOPBH#610'
                ) then 1
            else 0
        end
    ) as npv_ind,
    max(
        case
            when
                smart_data_element_all.concept_id in ('CHOPBH#611', 'CHOPBH#612', 'CHOPBH#613') then 1
            else 0
        end
    ) as follow_ind
from
    {{ref('smart_data_element_all')}} as smart_data_element_all
where
    smart_data_element_all.concept_id in (
        'CHOPBH#606',
        'CHOPBH#607',
        'CHOPBH#608',
        'CHOPBH#609',
        'CHOPBH#610',
        'CHOPBH#611',
        'CHOPBH#612',
        'CHOPBH#613')
    and smart_data_element_all.encounter_date >= '2018-01-01'
group by
    smart_data_element_all.note_key
