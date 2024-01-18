with flu_encounter as (
select
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.age_years,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.department_name,
    stg_encounter.provider_name,
    stg_encounter.pat_key,
    stg_encounter.visit_key
from {{ref('stg_encounter')}} as stg_encounter
    inner join {{source('cdw', 'provider_specialty')}} as provider_specialty
        on stg_encounter.provider_id = provider_specialty.prov_id
    left join {{ref('stg_encounter_telehealth')}} as stg_encounter_telehealth
        on stg_encounter.visit_key = stg_encounter_telehealth.visit_key
where
    stg_encounter.encounter_type_id in ('101', '50') -- office visit, appointment
    and stg_encounter.appointment_status_id in (2, 6) -- completed, arrived
    and (lower(stg_encounter.specialty_name) = 'pulmonary' -- any appointments with Pulm department
        or (lower(provider_specialty.spec_nm) = 'pulmonary' and stg_encounter.visit_type_id in ('1515', '1516')) -- PAPA clinic appointments with Pulm provider --noqa: L016
        )
--    and stg_encounter.visit_type_id not in ('3133', '7207', '4120', '4108', '2331', '8221', '8224', '7203', '8227', '9976', '8220', '8226', '4109', '7206', '4119', '4107', '3213', '2533', '2755', '2754', '8225', '3135', -- exclude sleep visit --noqa: L016
--                                  '3704', '2550', '3723', '3722', '3724') -- exclude aerodigestive visit
    and stg_encounter.department_id not in ('101022016', '101001610') -- remove virtua sleep lab and bgr pulmonary function departments --noqa: L016
    and stg_encounter_telehealth.visit_key is null -- encounter between Oct - Dec must be in person
group by
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.age_years,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.department_name,
    stg_encounter.provider_name,
    stg_encounter.pat_key,
    stg_encounter.visit_key
)
select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.start_date - interval '3 months' as flu_start_date,
    usnews_metadata_calendar.end_date as flu_end_date,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    base.mrn,
    base.patient_name,
    base.dob,
    flu_encounter.age_years,
    flu_encounter.encounter_date,
    base.pat_key,
    flu_encounter.visit_key
from
    {{ref('stg_usnews_pulm_asthma_base')}} as base
    inner join flu_encounter
        on base.pat_key = flu_encounter.pat_key
        and year(base.encounter_date) = year(flu_encounter.encounter_date)
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar -- join to get diagnosis code
--        on base.icd10_code = usnews_metadata_calendar.code
        on lower(usnews_metadata_calendar.question_number) = 'j15'
        and (flu_encounter.encounter_date >= usnews_metadata_calendar.start_date and flu_encounter.encounter_date <= usnews_metadata_calendar.end_date) -- noqa: L016
        and (flu_encounter.age_years >= usnews_metadata_calendar.age_gte and flu_encounter.age_years <= usnews_metadata_calendar.age_lt) -- noqa: L016
group by
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    base.mrn,
    base.patient_name,
    base.dob,
    flu_encounter.age_years,
    flu_encounter.encounter_date,
    base.pat_key,
    flu_encounter.visit_key
