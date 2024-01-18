select
    stg_monthly_cohort_jia.jia_id,
    stg_monthly_cohort_jia.visit_key,
    stg_monthly_cohort_jia.pat_key,
    stg_monthly_cohort_jia.mrn,
    stg_monthly_cohort_jia.csn,
    stg_monthly_cohort_jia.encounter_date,
    stg_monthly_cohort_jia.jia_ind,
    stg_rheumatology_jadas_scores.jadas,
    stg_patient.race,
    stg_patient.ethnicity,
    stg_patient.race_ethnicity,
    stg_patient.sex,
    stg_patient.patient_name,
    stg_patient.dob,
    stg_patient.preferred_language,
    patient.interpreter_needed_ind,
    equity_coi2.opportunity_lvl_coi_natl_norm,
    equity_coi2.opportunity_lvl_coi_state_norm,
    equity_coi2.opportunity_score_coi_natl_norm,
    equity_coi2.opportunity_score_coi_state_norm,
    stg_monthly_cohort_jia.monthyear,
    stg_monthly_cohort_jia.pat_population_review_dt
from
    {{ref('stg_monthly_cohort_jia')}} as stg_monthly_cohort_jia
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_monthly_cohort_jia.pat_key = stg_patient.pat_key
    inner join {{source('cdw', 'patient')}} as patient
        on stg_monthly_cohort_jia.pat_key = patient.pat_key
    left join {{ref('stg_rheumatology_jadas_scores')}} as stg_rheumatology_jadas_scores
        on stg_monthly_cohort_jia.visit_key = stg_rheumatology_jadas_scores.visit_key
    left join {{ref('patient_geospatial')}} as patient_geospatial
        on stg_monthly_cohort_jia.pat_key = patient_geospatial.pat_key
        and patient_geospatial.current_address_ind = 1
    left join {{ref('equity_coi2')}} as equity_coi2
        on patient_geospatial.census_tract_geoid_2010 = equity_coi2.census_tract_geoid_2010
        and equity_coi2.observation_year = 2015
