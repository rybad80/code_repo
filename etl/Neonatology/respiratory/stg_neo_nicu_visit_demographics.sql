/* Getting visit level demograhics by leaving out episode related fields */
select distinct
    visit_key,
    pat_key,
    patient_name,
    mrn,
    dob,
    sex,
    gestational_age_complete_weeks,
    gestational_age_remainder_days,
    birth_weight_grams,
    hospital_admit_date,
    hospital_discharge_date
from
    {{ ref('neo_nicu_episode') }}
