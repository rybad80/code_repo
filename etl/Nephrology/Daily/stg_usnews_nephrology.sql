with stage as (
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        to_date(submission_year, 'yyyy') as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        pat_key as primary_key,
        case when vaccinated_ind = 1 then pat_key else null end as num,
        pat_key as denom,
        null as cpt_code,
        null as procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g12_g13')}}
    where
        question_number = 'g12'
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        to_date(submission_year, 'yyyy') as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        pat_key as primary_key,
        case when vaccinated_ind = 1 then pat_key else null end as num,
        pat_key as denom,
        null as cpt_code,
        null as procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g12_g13')}}
    where
        question_number = 'g13'
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        encounter_date as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        pat_key as primary_key,
        num,
        denom,
        null as cpt_code,
        null as procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g20_g20_1_g21')}}
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        to_date(submission_year, 'yyyy') as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        primary_key,
        num,
        denom,
        cpt_code,
        procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g22')}}
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        to_date(month_year, 'mm-yyyy') as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        primary_key,
        num,
        denom,
        null as cpt_code,
        null as procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g23')}}
    where
        max_month_ktv is not null
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        to_date(submission_year, 'yyyy') as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        pat_key as primary_key,
        case when vaccinated_ind = 1 then pat_key else null end as num,
        pat_key as denom,
        null as cpt_code,
        null as procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g35')}}
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        to_date(submission_year, 'yyyy') as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        pat_key as primary_key,
        case when preemptive_transplant_ind = 1 then pat_key else null end as num,
        pat_key as denom,
        null as cpt_code,
        null as procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g31')}}
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        encounter_date as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        visit_key as primary_key,
        visit_key as num,
        null as denom,
        null as cpt_code,
        null as procedure_name,
        visit_key
    from
        {{ ref('stg_usnews_nephrology_g18_3')}}
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        primary_key,
        primary_key as num,
        null as denom,
        null as cpt_code,
        null as procedure_name,
        visit_key
    from
        {{ ref('stg_usnews_nephrology_g18_1')}}
    union all
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        to_date(submission_year, 'yyyy') as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        pat_key as primary_key,
        case when flu_ind = 1 then pat_key else null end as num,
        pat_key as denom,
        null as cpt_code,
        null as procedure_name,
        '0' as visit_key
    from
        {{ ref('stg_usnews_nephrology_g34') }}
)

select
    stage.pat_key,
    stage.patient_name,
    stage.mrn,
    stage.dob,
    stage.index_date,
    stage.submission_year,
    stage.division,
    stage.question_number,
    stage.metric_id,
    stage.primary_key,
    stage.num,
    stage.denom,
    stage.cpt_code,
    stage.procedure_name,
    stage.visit_key
from
    stage
