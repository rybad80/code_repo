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
    coalesce(stg_usnews_pulm_j10b.inpatient_encounter_date, stg_usnews_pulm_j10a.encounter_date)
        as metric_date,
    usnews_metadata_calendar.metric_id,
    case
        when usnews_metadata_calendar.metric_id = 'j10b1'
        then stg_usnews_pulm_j10a.mrn -- asthma outpatients
        else stg_usnews_pulm_j10b.mrn -- asthma outpatients who later got admitted
        end as num,
    case
        when usnews_metadata_calendar.metric_id = 'j10b'
        then stg_usnews_pulm_j10a.mrn -- asthma outpatients
        end as denom, -- j10b1 and j10b2 do not have denom calculation
    coalesce(stg_usnews_pulm_j10b.inpatient_encounter_date, stg_usnews_pulm_j10a.encounter_date)
        as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    null as department_specialty,
    null as provider_specialty,
    null as provider_name,
    stg_usnews_pulm_j10b.inpatient_visit_key as visit_key
from
    {{ref('stg_usnews_pulm_j10a')}} as stg_usnews_pulm_j10a
    left join {{ref('stg_usnews_pulm_j10b')}} as stg_usnews_pulm_j10b
        on stg_usnews_pulm_j10a.visit_key = stg_usnews_pulm_j10b.visit_key
    left join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.metric_id like 'j10b%'
        and stg_usnews_pulm_j10a.encounter_date
            between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
where num is not null or denom is not null -- removing null rows from j10b2
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
    stg_usnews_pulm_j10b.inpatient_encounter_date,
    stg_usnews_pulm_j10a.encounter_date,
    usnews_metadata_calendar.metric_id,
    stg_usnews_pulm_j10b.mrn,
    stg_usnews_pulm_j10b.inpatient_visit_key

union all

select
    'operational' as domain,
    'finance' as subdomain,
    stg_usnews_pulm_j10b.inpatient_visit_key as primary_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_usnews_pulm_j10b.mrn,
    stg_usnews_pulm_j10b.patient_name,
    stg_usnews_pulm_j10b.dob,
    coalesce(stg_usnews_pulm_j10c.encounter_date, stg_usnews_pulm_j10b.inpatient_encounter_date)
        as metric_date,
    usnews_metadata_calendar.metric_id,
    case
        when usnews_metadata_calendar.metric_id = 'j10c1'
        then stg_usnews_pulm_j10b.mrn -- asthma inpatients
        else stg_usnews_pulm_j10c.mrn -- asthma inpatients with a follow-up appointment
        end as num,
    case
        when usnews_metadata_calendar.metric_id = 'j10c'
        then stg_usnews_pulm_j10b.mrn -- asthma inpatients
        end as denom, -- j10c1 and j10c2 do not have denom calculation
    coalesce(stg_usnews_pulm_j10c.encounter_date, stg_usnews_pulm_j10b.inpatient_encounter_date)
        as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    stg_usnews_pulm_j10c.department_specialty,
    stg_usnews_pulm_j10c.provider_specialty,
    stg_usnews_pulm_j10c.provider_name,
    stg_usnews_pulm_j10c.fu_visit_key as visit_key
from
    {{ref('stg_usnews_pulm_j10b')}} as stg_usnews_pulm_j10b
    left join {{ref('stg_usnews_pulm_j10c')}} as stg_usnews_pulm_j10c
        on stg_usnews_pulm_j10b.inpatient_visit_key = stg_usnews_pulm_j10c.inpatient_visit_key
    left join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.metric_id like 'j10c%'
        and stg_usnews_pulm_j10b.inpatient_encounter_date
            between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
where num is not null or denom is not null -- removing null rows from j10c2
group by
    domain,
    subdomain,
    primary_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_usnews_pulm_j10b.mrn,
    stg_usnews_pulm_j10b.patient_name,
    stg_usnews_pulm_j10b.dob,
    stg_usnews_pulm_j10c.encounter_date,
    stg_usnews_pulm_j10b.inpatient_encounter_date,
    usnews_metadata_calendar.metric_id,
    stg_usnews_pulm_j10c.mrn,
    stg_usnews_pulm_j10c.department_specialty,
    stg_usnews_pulm_j10c.provider_specialty,
    stg_usnews_pulm_j10c.provider_name,
    stg_usnews_pulm_j10c.fu_visit_key
