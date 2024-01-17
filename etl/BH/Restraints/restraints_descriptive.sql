select
    stg_restraints_cohort.restraint_episode_key,
    case when stg_restraints_cohort.violent_restraint_ind = 1 then 'Violent Restraint Type'
         when stg_restraints_cohort.non_violent_restraint_ind = 1 then 'Non-Violent Restraint Type'
    end as event_type,
    stg_restraints_cohort.visit_key,
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_restraints_cohort.age_at_restraint as age_years,
    stg_restraints_cohort.department_name,
    stg_restraints_cohort.medical_clearance_72_hrs_ind,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date,
    stg_encounter_payor.payor_group,
    stg_encounter.sex,
    stg_patient.race_ethnicity,
    stg_patient.preferred_language,
    social_vulnerability_index.overall_category as svi_category,
    social_vulnerability_index.ses_category as svi_ses_category,
    equity_coi2.opportunity_lvl_coi_natl_norm as coi_level
from
    {{ ref('stg_restraints_cohort') }} as stg_restraints_cohort
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_restraints_cohort.visit_key = stg_encounter.visit_key
    inner join {{ ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter.visit_key = stg_encounter_payor.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_encounter.pat_key = stg_patient.pat_key
    left join {{source('ods', 'patient_geospatial_temp')}} as patient_geospatial_temp
        on stg_encounter.pat_key = patient_geospatial_temp.pat_key
    left join {{source('cdc_ods', 'social_vulnerability_index')}} as social_vulnerability_index
        on patient_geospatial_temp.census_tract_fips = social_vulnerability_index.fips
    left join {{ref('equity_coi2')}} as equity_coi2
        on patient_geospatial_temp.census_tract_fips = equity_coi2.census_tract_geoid_2010
        and year(restraint_start) between observation_year and observation_year + 5
