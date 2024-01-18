{{ config(meta = {
    'critical': false
}) }}

select
   'operational' as domain, --noqa: L029
   'pfex' as subdomain,
   'Likelihood to Recommend Top Box Score' as metric_name,
   pfex_all.survey_key as primary_key,
   pfex_all.visit_date as metric_date,
   pfex_all.tbs_ind as num,
   pfex_all.survey_key as denom,
   'sum' as num_calculation,
   'count' as denom_calculation,
   'percentage' as metric_type,
   'up' as desired_direction,
   'enterprise_ltr' as metric_id,
   pfex_all.survey_line_name,
   initcap(pfex_all.specialty_name) as specialty_name,
   pfex_all.department_name,
   pfex_all.dept_key,
   initcap(coalesce(department_care_network.department_center, pfex_all.department_name)) as department_center
from
    {{ref('pfex_all')}} as pfex_all
    inner join {{ref('lookup_press_ganey_question_id')}} as lookup_press_ganey_question_id
        on lower(pfex_all.survey_line_name) = lower(lookup_press_ganey_question_id.survey_line_name)
            and lower(pfex_all.question_id) = lower(lookup_press_ganey_question_id.question_id)
                and lower(lookup_press_ganey_question_id.metric_alias) = 'ltr'
     left join {{ref('department_care_network')}} as department_care_network
        on pfex_all.dept_key = department_care_network.dept_key
