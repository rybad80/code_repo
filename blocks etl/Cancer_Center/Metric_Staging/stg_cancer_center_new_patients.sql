select
    *
from
    {{ref ('cancer_center_visit')}}
where
    new_cancer_center_patient_ind = 1
