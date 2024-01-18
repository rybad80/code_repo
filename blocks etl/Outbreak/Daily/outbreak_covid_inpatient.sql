{{ config(meta = {
    'critical': true
}) }}

select
    encounter_inpatient.visit_key,
     encounter_inpatient.csn,
    encounter_inpatient.encounter_date,
    encounter_inpatient.mrn,
    stg_outbreak_covid_inpatient_question.isolation_desc,
    coalesce(stg_outbreak_covid_inpatient_question.covid19_exposure_ind, 0) as covid19_exposure_ind,
    coalesce(stg_outbreak_covid_inpatient_question.fever_last_24_hours_ind, 0) as fever_last_24_hours_ind,
    coalesce(stg_outbreak_covid_inpatient_question.respiratory_symptoms_ind, 0) as respiratory_symptoms_ind,
    coalesce(
        stg_outbreak_covid_inpatient_question.aerosol_generating_procedure_ind, 0
    ) as aerosol_generating_procedure_ind,
    coalesce(stg_outbreak_covid_inpatient_question.mis_c_concern_ind, 0) as mis_c_concern_ind,
    case
        when stg_inpatient_vaccine_opportunity.csn is not null then 1 else 0
    end as covid_vaccine_opportunity_ind,
    encounter_inpatient.pat_key,
    encounter_inpatient.pat_id
from
    {{ref('encounter_inpatient')}} as encounter_inpatient
    left join {{ref('stg_outbreak_covid_inpatient_question')}} as stg_outbreak_covid_inpatient_question
        on stg_outbreak_covid_inpatient_question.visit_key = encounter_inpatient.visit_key
        and last_order = 1
    left join {{ref('stg_inpatient_vaccine_opportunity')}} as stg_inpatient_vaccine_opportunity
        on encounter_inpatient.csn = stg_inpatient_vaccine_opportunity.csn
