{{ config(meta = {
    'critical': false
}) }}

with stage as (
    select
        patient_name,
        patient_mrn,
        patient_dob,
        index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        primary_key,
        num,
        denom,
        null as cpt,
        '0' as visit_key
    from
        {{ ref('stg_usnews_cancer_center_b18')}}
    union all
    select
        patient_name,
        patient_mrn,
        patient_dob,
        transplant_date as index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        primary_key,
        num,
        denom,
        null as cpt,
        '0' as visit_key
    from
        {{ ref('stg_usnews_cancer_center_b20_1')}}
    union
    select
        patient_name,
        mrn as patient_mrn,
        dob as patient_dob,
        index_date,
        submission_year,
        division,
        question_number,
        metric_id,
        primary_key,
        num,
        denom,
        null as cpt,
        '0' as visit_key
    from {{ref('stg_usnews_cancer_center_new_patients')}}
)

select
    stage.patient_name,
    stage.patient_mrn,
    stage.patient_dob,
    stage.index_date,
    stage.submission_year,
    stage.division,
    stage.question_number,
    stage.metric_id,
    stage.primary_key,
    stage.num,
    stage.denom,
    stage.cpt,
    stage.visit_key
from
    stage
