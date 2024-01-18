select
    stg_usnews_pulm_j10b.submission_year,
    stg_usnews_pulm_j10b.start_date,
    stg_usnews_pulm_j10b.end_date,
    stg_usnews_pulm_j10b.pat_key,
    stg_usnews_pulm_j10b.inpatient_visit_key,
    stg_usnews_pulm_j10b.mrn,
    stg_usnews_pulm_j10b.patient_name,
    stg_usnews_pulm_j10b.dob,
    stg_usnews_pulm_j10b.inpatient_encounter_date,
    stg_usnews_pulm_j10b.inpatient_discharge_date,
    stg_encounter.appointment_made_date,
    stg_encounter.encounter_date,
    stg_encounter.specialty_name as department_specialty,
    stg_encounter.provider_name,
    provider_specialty.spec_nm as provider_specialty,
    stg_encounter.visit_key as fu_visit_key
from
    {{ref('stg_usnews_pulm_j10b')}} as stg_usnews_pulm_j10b
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_usnews_pulm_j10b.pat_key = stg_encounter.pat_key
    inner join {{source('cdw', 'provider_specialty')}} as provider_specialty
        on stg_encounter.provider_id = provider_specialty.prov_id
where
    stg_encounter.encounter_type_id in ('50', '101')
    and (lower(stg_encounter.specialty_name) = 'pulmonary' -- any appointments with pulm department
            or (lower(provider_specialty.spec_nm) = 'pulmonary'
                and stg_encounter.visit_type_id in ('1515', '1516')
            ) -- papa clinic appointments with pulm provider
        )
    and stg_encounter.department_id not in ('101022016', '101001610') -- removing virtua sleep lab and bgr pulmonary functions departments --noqa: L016
    -- appointments made or occured after inpatient discharge date
    and (
        stg_encounter.appointment_made_date between
            date(stg_usnews_pulm_j10b.inpatient_discharge_date)
            and date(stg_usnews_pulm_j10b.inpatient_follow_up_date)
        or stg_encounter.encounter_date between
            date(stg_usnews_pulm_j10b.inpatient_discharge_date)
            and date(stg_usnews_pulm_j10b.inpatient_follow_up_date)
        )
group by
    stg_usnews_pulm_j10b.submission_year,
    stg_usnews_pulm_j10b.start_date,
    stg_usnews_pulm_j10b.end_date,
    stg_usnews_pulm_j10b.pat_key,
    stg_usnews_pulm_j10b.inpatient_visit_key,
    stg_usnews_pulm_j10b.mrn,
    stg_usnews_pulm_j10b.patient_name,
    stg_usnews_pulm_j10b.dob,
    stg_usnews_pulm_j10b.inpatient_encounter_date,
    stg_usnews_pulm_j10b.inpatient_discharge_date,
    stg_encounter.original_appointment_made_date,
    stg_encounter.appointment_made_date,
    stg_encounter.encounter_date,
    department_specialty,
    stg_encounter.provider_name,
    provider_specialty,
    fu_visit_key
