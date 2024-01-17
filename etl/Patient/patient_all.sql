select
    stg_patient.patient_key,
    stg_patient.pat_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.pat_id,
    stg_patient.dob,
    stg_patient.sex,
    stg_patient.gender_identity,
    stg_patient.sex_assigned_at_birth,
    stg_patient.current_age,
    stg_patient.email_address,
    stg_patient.home_phone,
    stg_patient.mailing_address_line1,
    stg_patient.mailing_address_line2,
    stg_patient.mailing_city,
    stg_patient.mailing_state,
    stg_patient.mailing_zip,
    stg_patient.county,
    stg_patient.gestational_age_complete_weeks,
    stg_patient.gestational_age_remainder_days,
    stg_patient.birth_weight_kg,
    stg_patient.race,
    stg_patient.ethnicity,
    stg_patient.race_ethnicity,
    stg_patient.preferred_language,
    stg_patient.preferred_name,
    stg_patient.texting_opt_in_ind,
    stg_patient.deceased_ind,
    stg_patient.death_date,
    stg_patient.record_state,
    stg_patient.current_record_ind,
    stg_patient.interpreter_needed,
    stg_patient_payor.payor_name,
    stg_patient_payor.payor_group,
    stg_patient_payor.start_date as payor_start_date,
    stg_patient_pcp_attribution.pcp_location as current_pcp_location,
    stg_patient_pcp_attribution.pcp_provider as current_pcp_provider,
    stg_mychop_status.mychop_activation_ind,
    stg_mychop_status.mychop_declined_ind
from
    {{ref('stg_patient')}} as stg_patient
    left join {{ref('stg_mychop_status')}} as stg_mychop_status
        on stg_mychop_status.pat_key = stg_patient.pat_key
    left join {{ref('stg_patient_payor')}} as stg_patient_payor
        on stg_patient_payor.pat_key = stg_patient.pat_key
    left join {{ref('stg_patient_pcp_attribution')}} as stg_patient_pcp_attribution
        on stg_patient_pcp_attribution.pat_key = stg_patient.pat_key
            and current_date between
            stg_patient_pcp_attribution.start_date
            and stg_patient_pcp_attribution.end_date
