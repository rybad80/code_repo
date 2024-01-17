with access_during_submission_year as (
select
    submission_year, 
    pat_key, 
    question_group, 
    service_date as access_service_date
from {{ ref('stg_usnews_nephrology_billing_g22')}}
where 
    during_year_ind = 1
    and excluded_ind != 1
group by
    submission_year, 
    pat_key, 
    question_group, 
    service_date
    
),

max_access_date as (
    select
        submission_year, 
        pat_key, 
        question_group, 
        max(service_date) as access_service_date,
        max(during_year_ind) as max_during_year_ind,
        case when max_during_year_ind = 1 then 1 else 0 end as excluded_ind 
    from {{ ref('stg_usnews_nephrology_billing_g22')}}
    group by
        submission_year,
        pat_key,
        question_group
),

accesses as (
    select distinct 
    stage.submission_year, 
    stage.division,
    stage.pat_key, 
    stage.patient_name,
    stage.mrn,
    stage.dob,
    /* Takes the last access age of the patient, by question group, if there is more than 
    one access in that group during the submission year. This is done so that the patient 
    falls into only one age bucket if there are accesses that would otherwise fall into two 
    different metric_ids (G22a vs G22b and G22d vs G22e) This does not impact whether a patient
    would fall into G22(a or b), G22c, or G22(d or e) - where the instructions specifically 
    specify that a patient can fall into all three */
    max(stage.age_years) over (partition by stage.submission_year, stage.pat_key, stage.question_group) 
        as age_years,
    stage.question_group,
    stage.cpt_code,
    stage.procedure_name,
    access_service_date
from {{ ref('stg_usnews_nephrology_billing_g22')}} as stage
    left join access_during_submission_year 
        on stage.pat_key = access_during_submission_year.pat_key
        and stage.submission_year = access_during_submission_year.submission_year
        and stage.question_group = access_during_submission_year.question_group
        and stage.service_date = access_during_submission_year.access_service_date
where access_service_date is not null
group by
    stage.submission_year, 
    stage.division,
    stage.pat_key, 
    stage.patient_name,
    stage.mrn,
    stage.dob,
    stage.age_years,
    stage.question_group,
    stage.cpt_code,
    stage.procedure_name,
    access_service_date

union all

select distinct 
stage.submission_year,
stage.division,
stage.pat_key,
stage.patient_name,
stage.mrn,
stage.dob,
stage.age_years,
stage.question_group,
stage.cpt_code,
stage.procedure_name,
access_service_date
from {{ ref('stg_usnews_nephrology_billing_g22')}} as stage
    left join max_access_date
        on stage.pat_key = max_access_date.pat_key
        and stage.submission_year = max_access_date.submission_year
        and stage.question_group = max_access_date.question_group
        and stage.service_date = max_access_date.access_service_date
        and max_access_date.excluded_ind = 0
where access_service_date is not null
group by
    stage.submission_year, 
    stage.division,
    stage.pat_key, 
    stage.patient_name,
    stage.mrn,
    stage.dob,
    stage.age_years,
    stage.question_group,
    stage.cpt_code,
    stage.procedure_name, 
    access_service_date
),

question_grouper as (
select
    accesses.submission_year, 
    accesses.division,
    accesses.pat_key,
    accesses.patient_name,
    accesses.mrn,
    accesses.dob,
    accesses.age_years,
    accesses.question_group, 
    accesses.access_service_date,
    accesses.cpt_code,
    accesses.procedure_name,
    accesses.age_years as age,
    case when age < 5 and question_group = 'hd_central_caths' then 'g22a'
        when age < 18 and question_group = 'hd_central_caths' then 'g22b' 
        when age >= 10 and age < 18 and question_group = 'hd_fistula_graft' then 'g22c' 
        when age < 5 and question_group = 'pd_cath' then 'g22d'
        when age < 18 and question_group = 'pd_cath' then 'g22e' 
        end as question_number,
    {{
    dbt_utils.surrogate_key([
        'accesses.pat_key',
        'access_service_date'
        ])
    }} as primary_key
from accesses
where question_number is not null
group by
    accesses.submission_year, 
    accesses.division,
    accesses.pat_key, 
    accesses.patient_name,
    accesses.mrn,
    accesses.dob,
    accesses.age_years,
    accesses.question_group, 
    accesses.access_service_date,
    accesses.age_years,
    accesses.cpt_code,
    accesses.procedure_name
)

--Accesses
select
    question_grouper.*,
    question_number||1 as metric_id,
    primary_key as num,
    null as denom
from 
    question_grouper

union all

--Patients
select
    question_grouper.*,
    question_number||2 as metric_id,
    pat_key as num,
    null as denom
from 
    question_grouper

union all

--Ratio
select
    question_grouper.*,
    question_number as metric_id,
    primary_key as num,
    pat_key as denom
from 
    question_grouper