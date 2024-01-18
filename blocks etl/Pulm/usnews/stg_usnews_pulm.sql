select
    'operational' as domain,
    'finance' as subdomain,
    pat_key as primary_key,
    division,
    question_number,
    metric_name,
    submission_year,
    mrn,
    patient_name,
    dob,
    coalesce(received_date, documented_date, most_recent_encounter_date) as metric_date,
    metric_id,
    case when received_date is not null or documented_date is not null then mrn end as num,
    mrn as denom,
    coalesce(received_date, documented_date, most_recent_encounter_date) as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    null as department_specialty,
    null as provider_specialty,
    null as provider_name,
    '0' as visit_key
from
    {{ref('stg_usnews_pulm_flu')}}

union all

select
    domain,
    subdomain,
    primary_key,
    division,
    question_number,
    metric_name,
    submission_year,
    mrn,
    patient_name,
    dob,
    metric_date,
    metric_id,
    num,
    denom,
    index_date,
    cpt_code,
    procedure_name,
    subsequent_date,
    department_specialty,
    provider_specialty,
    provider_name,
    visit_key
from
    {{ref('stg_usnews_pulm_j10a_j10d_j10e')}}

union all

select
    domain,
    subdomain,
    primary_key,
    division,
    question_number,
    metric_name,
    submission_year,
    mrn,
    patient_name,
    dob,
    metric_date,
    metric_id,
    num,
    denom,
    index_date,
    cpt_code,
    procedure_name,
    subsequent_date,
    department_specialty,
    provider_specialty,
    provider_name,
    visit_key
from
    {{ref('stg_usnews_pulm_j10b_j10c')}}

union all

select
    'operational' as domain,
    'finance' as subdomain,
    stg_cf_ogtt_cohort.pat_key as primary_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_cf_ogtt_cohort.mrn,
    stg_cf_ogtt_cohort.patient_name,
    stg_patient.dob,
    coalesce(stg_cf_ogtt_cohort.ogtt_date, to_date(submission_year, 'yyyy')) as metric_date,
    usnews_metadata_calendar.metric_id,
    case
        when
            (usnews_metadata_calendar.metric_id = 'j23a' -- numerator
                or usnews_metadata_calendar.metric_id = 'j23') -- percentage
            and ogtt_date is not null -- patients with ogtt in a calendar year
        then stg_cf_ogtt_cohort.mrn
        when usnews_metadata_calendar.metric_id = 'j23b' -- denominator
        then stg_cf_ogtt_cohort.mrn -- any patients on CF list
        end as num,
    case
        when usnews_metadata_calendar.metric_id = 'j23'
        then stg_cf_ogtt_cohort.mrn -- any patients on CF list
        end as denom, -- j23a and j23b do not have denom calculation
    coalesce(stg_cf_ogtt_cohort.ogtt_date, to_date(submission_year, 'yyyy')) as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    null as department_specialty,
    null as provider_specialty,
    null as provider_name,
    null as visit_key
from
    {{ref('stg_cf_ogtt_cohort')}} as stg_cf_ogtt_cohort
    left join {{ref('stg_patient')}} as stg_patient
        on stg_cf_ogtt_cohort.pat_key = stg_patient.pat_key
    left join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.metric_id like 'j23%'
        and stg_cf_ogtt_cohort.cy = usnews_metadata_calendar.submission_year - 1
where stg_cf_ogtt_cohort.min_age >= 10
