{{ config(meta = {
    'critical': false
}) }}

select
    patient_name,
    patient_mrn,
    patient_dob,
    transplant_date,
    question_number,
    submission_year,
    division,
    metric_id,
    {{
    dbt_utils.surrogate_key([
    'patient_mrn',
    'transplant_date',
    'question_number'
    ])
    }} as primary_key,
    primary_key as num,
    null as denom
from
    {{ ref('stg_cancer_center_bmt_allo_identical')}}
    inner join {{ ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.question_number = 'b20.1'
        and transplant_date between start_date and end_date
where
    ((metric_id = 'b20.1a' and lower(relationship) = 'sibling')
    or (metric_id = 'b20.1b' and lower(relationship) = 'sibling' and death_within_100_days_ind = 0)
    or (metric_id = 'b20.1c' and lower(relationship) = 'unrelated')
    or (metric_id = 'b20.1d' and lower(relationship) = 'unrelated' and death_within_100_days_ind = 0))
    and transplant_date + 100 <= current_date
