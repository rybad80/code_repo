select
    patient_name,
    patient_mrn,
    patient_dob,
    disease_name,
    disease_classification,
    disease_category,
    relationship,
    donor_match_grade,
    transplant_date,
    death_date,
    case when date(death_date) - date(transplant_date) < 100 then 1
    else 0 end as death_within_100_days_ind
from
    {{ ref('cancer_center_bmt_transplants')}}
where allogeneic_matched_donor_transplant_ind = 1
and donor_match_grade = '10/10'
and malignancy_history_ind = 1
and first_transplant_date_ind = 1
and lt_21_ind = 1
