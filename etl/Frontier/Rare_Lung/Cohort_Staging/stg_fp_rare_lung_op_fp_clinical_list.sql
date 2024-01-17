select
    patient_all.mrn
from
    {{ source('cdw', 'patient_list') }} as patient_list
    inner join {{ ref('patient_all') }} as patient_all
        on patient_all.pat_key = patient_list.pat_key
    inner join {{source('cdw','patient_list_info')}} as patient_list_info
        on patient_list_info.pat_lst_info_key = patient_list.pat_lst_info_key
where
    patient_list_info.pat_lst_info_id = 485955  -- 'frontier: clinical cohort' Epic patient list
    and patient_list.cur_rec_ind = 1            -- extract only current records
