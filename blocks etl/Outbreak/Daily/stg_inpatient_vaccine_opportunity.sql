{{ config(meta = {
    'critical': true
}) }}

select
    encounter_inpatient.csn
from
    {{ref('encounter_inpatient')}} as encounter_inpatient
    /*anti-join patients with a prior dose*/
    left join {{ref('outbreak_covid_vaccination')}} as outbreak_covid_vaccination
        on outbreak_covid_vaccination.pat_id = encounter_inpatient.pat_id
        and outbreak_covid_vaccination.earliest_known_dose_date < encounter_inpatient.encounter_date
    inner join {{ref('lookup_covid_vaccine_eligibility_age')}}
    as lookup_covid_vaccine_eligibility_age
        on encounter_inpatient.encounter_date
        between lookup_covid_vaccine_eligibility_age.start_date
        and coalesce(lookup_covid_vaccine_eligibility_age.end_date, current_date)
where
    /*still admitted when IP vaccinations began*/
    coalesce(
        encounter_inpatient.hospital_discharge_date,
        current_date) >= '2021-7-25'
    /*met age cutoff at time of encounter*/
    and encounter_inpatient.age_years
    >= lookup_covid_vaccine_eligibility_age.minimum_eligibility_age
    and outbreak_covid_vaccination.pat_id is null
