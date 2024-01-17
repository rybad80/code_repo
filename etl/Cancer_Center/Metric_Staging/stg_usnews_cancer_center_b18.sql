{{ config(meta = {
    'critical': false
}) }}

with stage as (
    select
        patient_name,
        patient_mrn,
        patient_dob,
        transplant_date as index_date,
        question_number,
        submission_year,
        division,
        metric_id
    from
        {{ ref('cancer_center_bmt_transplants')}}
        inner join {{ ref('usnews_metadata_calendar')}}
            on transplant_date between start_date and end_date
    where
        malignancy_history_ind = 1
        and lt_21_ind = 1
        and (
            (metric_id = 'b18a2' and autologous_stem_cell_transplant_ind = 1)
            or (metric_id = 'b18b2' and (allogeneic_matched_donor_transplant_ind = 1 or haplo_transplant_ind = 1))
        )

    union all

    select
        patient_name,
        patient_mrn,
        patient_dob,
        index_date,
        question_number,
        submission_year,
        division,
        metric_id
    from
        {{ ref('stg_cancer_center_bmt_cellular_therapy')}}
        inner join {{ ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
            on index_date between start_date and end_date
    where
        lt_21_ind = 1
        and usnews_metadata_calendar.metric_id = 'b18c2'
)

select
    stage.*,
    {{
    dbt_utils.surrogate_key([
    'patient_mrn',
    'index_date',
    'question_number'
    ])
    }} as primary_key,
    primary_key as num,
    null as denom
from
    stage
