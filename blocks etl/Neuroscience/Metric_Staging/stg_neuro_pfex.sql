select
    case when question_id in ('O15')
    then 'Staff Worked Together Top Box Score (Spec)**'
    when question_id in ('O4')
    then 'Likelihood to Recommend Top Box Score (Spec)**'
    else 'Metric is not ready for use'
    end as metric_name,
    case when lower(specialty_name) = 'leukodystropy' then 'NEUROLOGY'
    else specialty_name
    end as drill_down,
    visit_key,
    survey_key,
    pat_key,
    dept_key,
    provider_name,
    visit_date,
    survey_sent_date,
    survey_returned_date,
    specialty_name,
    department_name,
    department_id,
    survey_line_name,
    survey_line_id,
    section_name,
    question_name,
    question_id,
    response_text,
    tbs_ind,
    case when question_id = 'O15' then 'neuro_staff_spec'
        when question_id = 'O4' then 'neuro_ltr_spec'
        else 'Metric is not ready for use'
        end as metric_id
from
    {{ ref('pfex_all')}}
where
    lower(survey_line_name) = 'specialty care'
    and lower(specialty_name) in (
        'neurology',
        'neurosurgery',
        'leukodystropy'
        )
