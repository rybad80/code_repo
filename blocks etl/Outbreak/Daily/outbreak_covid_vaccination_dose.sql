{{ config(meta = {
    'critical': true
}) }}

select
    stg_outbreak_covid_vaccination_dose.immune_id,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.dob,
    stg_outbreak_covid_vaccination_dose.received_date,
    case
        when stg_outbreak_covid_vaccination_dose.primary_dose_admin_ind = 1
        then stg_outbreak_covid_vaccination_dose.dose_description
    end as dose_description,
    stg_outbreak_covid_vaccination_dose.administration_location,
    stg_outbreak_covid_vaccination_dose.manufacturer_name,
    stg_outbreak_covid_vaccination_dose.internal_admin_ind as internal_administration_ind,
    stg_outbreak_covid_vaccination_dose.inpatient_administration_ind,
    stg_outbreak_covid_vaccination_dose.order_id,
    stg_outbreak_covid_vaccination_dose.order_csn,
    stg_patient.pat_key,
    stg_outbreak_covid_vaccination_dose.pat_id
from
    {{ref('stg_outbreak_covid_vaccination_dose')}}
    as stg_outbreak_covid_vaccination_dose
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_id = stg_outbreak_covid_vaccination_dose.pat_id
