with stage as (
    select
        pat_key,
        patient_name,
        mrn,
        dob,
        dos as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        visit_key as primary_key,
        num,
        denom,
        null as cpt_code,
        null as procedure_name,
        visit_key
    from {{ref('stg_usnews_cardiology_e22')}}

    union all

    select
        pat_key,
        patient_name,
        mrn,
        dob,
        dos as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        visit_key as primary_key,
        num,
        denom,
        null as cpt_code,
        null as procedure_name,
        visit_key
    from {{ref('stg_usnews_cardiology_e42')}}

    union all

    select
        pat_key,
        patient_name,
        mrn,
        dob,
        dos as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        visit_key as primary_key,
        num,
        denom,
        null as cpt_code,
        null as procedure_name,
        visit_key
    from {{ ref('stg_usnews_cardiology_e37_1')}}

    union all

    select
        pat_key,
        patient_name,
        mrn,
        dob,
        dos as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        visit_key as primary_key,
        num,
        denom,
        null as cpt_code,
        null as procedure_name,
        visit_key
    from {{ref('stg_usnews_cardiology_e37_3')}}

    union all

    select
        pat_key,
        patient_name,
        mrn,
        dob,
        dos as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        visit_key as primary_key,
        num,
        denom,
        null as cpt_code,
        null as procedure_name,
        visit_key
    from {{ref('stg_usnews_cardiology_e37_4')}}

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
from stage
