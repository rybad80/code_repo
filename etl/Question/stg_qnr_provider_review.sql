--PROs Provider Review for all encounters 

select
    stg_encounter.visit_key,
    clinical_concept.concept_id,
    clinical_concept.concept_desc,
    employee.full_nm as sde_entered_employee,
    smart_data_element_info.entered_dt as sde_entered_date,
    case
        when smart_data_element_value.elem_val = 0
            then 'Reviewed asynchronous from encounter'
        when smart_data_element_value.elem_val = 1
            then 'Reviewed with patient/caretaker, no changes to plan of care'
        when smart_data_element_value.elem_val = 2
            then 'Reviewed with patient/caretaker, influenced plan of care'
        when smart_data_element_value.elem_val = 3
            then 'Review attempted but data incomplete'
    end as sde_value,
    1 as sde_use_ind,
    case when smart_data_element_value.elem_val = 0 then 1 else 0 end as sde_0_ind,
    case when smart_data_element_value.elem_val = 1 then 1 else 0 end as sde_1_ind,
    case when smart_data_element_value.elem_val = 2 then 1 else 0 end as sde_2_ind,
    case when smart_data_element_value.elem_val = 3 then 1 else 0 end as sde_3_ind

from {{ ref('stg_encounter') }} as stg_encounter
    left join {{source('cdw', 'smart_data_element_info')}} as smart_data_element_info
        on stg_encounter.visit_key = smart_data_element_info.visit_key
    left join {{source('cdw', 'smart_data_element_value')}} as smart_data_element_value
        on smart_data_element_info.sde_key = smart_data_element_value.sde_key
    left join {{source('cdw', 'clinical_concept')}} as clinical_concept
        on smart_data_element_info.concept_key = clinical_concept.concept_key
    left join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = smart_data_element_info.emp_key

where
    smart_data_element_value.elem_val is not null
    and clinical_concept.concept_id = 'CHOP#5844'

group by
    stg_encounter.visit_key,
    clinical_concept.concept_id,
    clinical_concept.concept_desc,
    sde_entered_employee,
    sde_entered_date,
    sde_value,
    sde_use_ind,
    sde_0_ind,
    sde_1_ind,
    sde_2_ind,
    sde_3_ind
