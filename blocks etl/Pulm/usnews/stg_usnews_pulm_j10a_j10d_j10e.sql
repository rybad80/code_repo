select
    'operational' as domain,
    'finance' as subdomain,
    stg_usnews_pulm_j10a.visit_key as primary_key,
    stg_usnews_pulm_j10a.division,
    stg_usnews_pulm_j10a.question_number,
    stg_usnews_pulm_j10a.metric_name,
    stg_usnews_pulm_j10a.submission_year,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.encounter_date as metric_date,
    stg_usnews_pulm_j10a.metric_id,
    stg_usnews_pulm_j10a.mrn as num,
    null as denom,
    stg_usnews_pulm_j10a.encounter_date as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    stg_usnews_pulm_j10a.department_specialty,
    stg_usnews_pulm_j10a.provider_specialty,
    stg_usnews_pulm_j10a.provider_name,
    stg_usnews_pulm_j10a.visit_key
from
    {{ref('stg_usnews_pulm_j10a')}} as stg_usnews_pulm_j10a

union all

select
    'operational' as domain,
    'finance' as subdomain,
    stg_usnews_pulm_j10a.visit_key as primary_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.encounter_date as metric_date,
    usnews_metadata_calendar.metric_id,
    stg_usnews_pulm_j10a.mrn as num,
    null as denom,
    stg_usnews_pulm_j10a.encounter_date as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    stg_usnews_pulm_j10a.department_specialty,
    stg_usnews_pulm_j10a.provider_specialty,
    stg_usnews_pulm_j10a.provider_name,
    stg_usnews_pulm_j10a.visit_key
from
    {{ref('stg_usnews_pulm_j10a')}} as stg_usnews_pulm_j10a
left join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
    on usnews_metadata_calendar.metric_id = 'j10d'
    and stg_usnews_pulm_j10a.encounter_date
        between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
where
    stg_usnews_pulm_j10a.age_years >= 5 and stg_usnews_pulm_j10a.age_years < 21
group by
    stg_usnews_pulm_j10a.visit_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.encounter_date,
    usnews_metadata_calendar.metric_id,
    stg_usnews_pulm_j10a.department_specialty,
    stg_usnews_pulm_j10a.provider_specialty,
    stg_usnews_pulm_j10a.provider_name

union all

select
    'operational' as domain,
    'finance' as subdomain,
    stg_usnews_pulm_j10a.visit_key as primary_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    coalesce(stg_usnews_pulm_j10e.max_act_date, stg_usnews_pulm_j10a.encounter_date)
        as metric_date,
    usnews_metadata_calendar.metric_id,
    case
        when usnews_metadata_calendar.metric_id = 'j10e1'
        then stg_usnews_pulm_j10a.mrn -- asthma outpatients eligible for ACT
        else stg_usnews_pulm_j10e.mrn -- asthma outpatients with ACT documented
        end as num,
    case
        when usnews_metadata_calendar.metric_id = 'j10e'
        then stg_usnews_pulm_j10a.mrn -- asthma outpatients with ACT documented
        end as denom, -- j10e1 and j10e2 do not have denom calculation
    coalesce(stg_usnews_pulm_j10e.max_act_date, stg_usnews_pulm_j10a.encounter_date)
        as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    stg_usnews_pulm_j10e.department_specialty,
    stg_usnews_pulm_j10e.provider_specialty,
    stg_usnews_pulm_j10e.provider_name,
    stg_usnews_pulm_j10e.visit_key
from
    {{ref('stg_usnews_pulm_j10a')}} as stg_usnews_pulm_j10a
    left join {{ref('stg_usnews_pulm_j10e')}} as stg_usnews_pulm_j10e
        on stg_usnews_pulm_j10a.visit_key = stg_usnews_pulm_j10e.visit_key
    left join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.metric_id like 'j10e%'
        and stg_usnews_pulm_j10a.encounter_date
            between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
where
    stg_usnews_pulm_j10a.age_years >= 5 and stg_usnews_pulm_j10a.age_years < 21
    and (num is not null or denom is not null)
group by
    domain,
    subdomain,
    primary_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10e.max_act_date,
    stg_usnews_pulm_j10a.encounter_date,
    usnews_metadata_calendar.metric_id,
    stg_usnews_pulm_j10e.mrn,
    stg_usnews_pulm_j10e.department_specialty,
    stg_usnews_pulm_j10e.provider_specialty,
    stg_usnews_pulm_j10e.provider_name,
    stg_usnews_pulm_j10e.visit_key
