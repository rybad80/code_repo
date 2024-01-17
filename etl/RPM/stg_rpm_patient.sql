select
    stg_patient_ods.patient_key,
    stg_patient_ods.pat_id,
    stg_patient_ods.patient_name,
    stg_patient_ods.current_age,
    stg_patient_ods.email_address,
    stg_patient_ods.home_phone,
    case
        when stg_patient_ods.current_age >= 13 then 1
        else 0
    end as pt_13_and_over_ind,
    case
        when stg_patient_ods.current_age >= 16 then 1
        else 0
    end as pt_16_and_over_ind,
    case
        when stg_patient_ods.current_age >= 18 then 1
        else 0
    end as pt_18_and_over_ind
from
    {{ ref('stg_patient_ods') }} as stg_patient_ods
where
    stg_patient_ods.current_record_ind = 1
