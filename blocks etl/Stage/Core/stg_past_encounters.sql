{{ config(meta = {
    'critical': true
}) }}

/* This staging table contains primary care, specialty care, urgent care, and inpatient encounters.
    It does not contain ED encounters because the stage table will be used in ed_encounter.
*/

select
    visit_key,
    pat_key,
    patient_name,
    mrn,
    dob,
    csn,
    encounter_date,
    age_years,
    department_name,
    department_id,
    prov_key
from
    {{ref('stg_encounter_outpatient_raw')}}
where
    encounter_date < current_date
    and (
        primary_care_ind = 1
        or specialty_care_ind = 1
        or urgent_care_ind = 1
        )

union all

select
    stg_encounter_inpatient.visit_key,
    stg_encounter.pat_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.age_years,
    stg_encounter_inpatient.admission_department_group as department_name,
    stg_encounter_inpatient.admission_department_center_id as department_id,
    stg_encounter.prov_key
from
    {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = stg_encounter_inpatient.visit_key
where
    stg_encounter.hospital_discharge_date is not null
    and stg_encounter_inpatient.ed_ind = 0
