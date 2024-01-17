{{ config(
    meta={
        'critical': true
    }
) }}

select
    dim_patient.patient_key,
    dim_patient.patient_name,
    dim_patient.mrn,
    dim_patient.pat_id,
    dim_patient.dob,
    dim_patient.sex,
    dim_patient.gender_identity,
    dim_patient.sex_assigned_at_birth,
    dim_patient.current_age,
    dim_patient.email_address,
    dim_patient.home_phone,
    dim_patient.mailing_address_line1,
    dim_patient.mailing_address_line2,
    dim_patient.mailing_city,
    dim_patient.mailing_state,
    dim_patient.mailing_zip,
    dim_patient.county,
    dim_patient.country,
    dim_patient.gestational_age_complete_weeks,
    dim_patient.gestational_age_remainder_days,
    dim_patient.birth_weight_kg,
    dim_patient.preferred_language,
    dim_patient.preferred_name,
    dim_patient.race,
    dim_patient.ethnicity,
    dim_patient.race_ethnicity,
    dim_patient.interpreter_needed,
    dim_patient.texting_opt_in_ind,
    dim_patient.deceased_ind,
    dim_patient.death_date,
    dim_patient.record_state,
    dim_patient.current_record_ind,
    dim_patient.create_source
from
    {{ref('dim_patient')}} as dim_patient
    left join {{source('clarity_ods','patient_3')}} as patient_3
        on patient_3.pat_id = dim_patient.pat_id
    left join {{source('clarity_ods','patient_type')}} as patient_type
        on patient_type.pat_id = dim_patient.pat_id
        and patient_type.line = 1 -- Test patients will only have 1 line
    left join {{ref('lookup_test_patients')}} as lookup_test_patients
        on lookup_test_patients.pat_id = dim_patient.pat_id
where
    lower(dim_patient.patient_name) not like 'client%'
    and lower(dim_patient.patient_name) not like 'combined%'
    and lower(dim_patient.patient_name) not like 'research%'
    and lower(dim_patient.patient_first_name) not like 'research%'
    and lower(dim_patient.patient_name) not like 'unallocated%'
    and lower(dim_patient.patient_name) not like 'zunapplied%'
    and lower(coalesce(dim_patient.patient_first_name, '')) not like '*%'
    and lower(coalesce(dim_patient.patient_last_name, '')) not like '*%'
    and dim_patient.patient_name is not null
    and coalesce(patient_type.patient_type_c, '0') != '16' --Test Patient
    and dim_patient.mrn is not null
    and not regexp_like(dim_patient.patient_name, '[0-9][0-9]')
    and lookup_test_patients.pat_id is null
    and coalesce(patient_3.is_test_pat_yn, 'N') = 'N'
