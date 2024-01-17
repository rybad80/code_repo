select
	stg_diabetes_depression_screening.diabetes_reporting_month,
	stg_diabetes_depression_screening.patient_key,
    stg_diabetes_depression_screening.phq2_screened,
    stg_diabetes_depression_screening.positive_phq2,
    stg_diabetes_depression_screening.phq2_screened_dt,
    stg_diabetes_depression_screening.phq89_screened,
    stg_diabetes_depression_screening.positive_phq89,
    stg_diabetes_depression_screening.phq89_screened_dt,
    stg_diabetes_depression_screening.depression_screened_dt,
    stg_diabetes_depression_screening.depression_screened_ind,
    stg_diabetes_depression_screening.positive_depression_ind,
    stg_diabetes_depression_screening.positive_suicide_ind,
    stg_diabetes_depression_screening.flo_action_taken,
    stg_diabetes_depression_screening.bpa_action_taken,
	stg_diabetes_depression_screening.psych_enc_ind,
	stg_diabetes_depression_screening.sw_enc_ind,
	stg_diabetes_depression_screening.treatment_ind,
	case when stg_diabetes_depression_screening.positive_depression_ind = 1
           then coalesce(stg_diabetes_depression_screening.flo_action_taken,
                        stg_diabetes_depression_screening.bpa_action_taken,
                        stg_diabetes_depression_screening.treatment_ind,
                        0)
           else 0 end as depression_action_taken_ind
from {{ref('stg_diabetes_depression_screening')}} as stg_diabetes_depression_screening
