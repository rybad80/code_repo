{{ config(meta = {
    'critical': false
}) }}

select
    case when pfex_all.question_id = 'O2' and lower(pfex_all.survey_line_name) = 'inpatient pediatric'
        then 'Staff Worked Together Top Box Score (IP)**'
        when pfex_all.question_id = 'O4' and lower(pfex_all.survey_line_name) = 'inpatient pediatric'
        then 'Likelihood to Recommend Top Box Score (IP)**'
        when pfex_all.question_id = 'O15' and lower(pfex_all.survey_line_name) = 'specialty care'
        then 'Staff Worked Together Top Box Score (Spec)**'
        when pfex_all.question_id = 'O4' and lower(pfex_all.survey_line_name) = 'specialty care'
        then 'Likelihood to Recommend Top Box Score (Spec)**'
        else 'Metric is not ready for use'
        end as metric_name,
    pfex_all.visit_key,
    pfex_all.survey_key,
    pfex_all.pat_key,
    pfex_all.dept_key,
    pfex_all.provider_name,
    pfex_all.visit_date,
    pfex_all.survey_sent_date,
    pfex_all.survey_returned_date,
    pfex_all.specialty_name,
    pfex_all.department_name,
    pfex_all.department_id,
    pfex_all.survey_line_name,
    pfex_all.survey_line_id,
    pfex_all.section_name,
    pfex_all.question_name,
    pfex_all.question_id,
    pfex_all.response_text,
    pfex_all.tbs_ind,
    case when pfex_all.question_id = 'O2' and lower(pfex_all.survey_line_name) = 'inpatient pediatric'
        then 'onco_staff_ip'
        when pfex_all.question_id = 'O4' and lower(pfex_all.survey_line_name) = 'inpatient pediatric'
        then 'onco_ltr_ip'
        when pfex_all.question_id = 'O15' and lower(pfex_all.survey_line_name) = 'specialty care'
        then 'onco_staff_spec'
        when pfex_all.question_id = 'O4' and lower(pfex_all.survey_line_name) = 'specialty care'
        then 'onco_ltr_spec'
        else 'Metric is not ready for use'
        end as metric_id
from
    {{ ref('pfex_all') }} as pfex_all
    inner join {{ ref('lookup_service_line_departments')}} as lookup_service_line_departments
        on pfex_all.department_id = lookup_service_line_departments.department_id
where
    lower(lookup_service_line_departments.service_line) = 'oncology'