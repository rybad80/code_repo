select
    'operational' as domain, --noqa: L029
    'pfex' as subdomain,
    'Move Through Visit Section Top Box Score' as metric_name,
    pfex_all.survey_key as primary_key,
    initcap(pfex_all.specialty_name) as drill_down_one,
    initcap(coalesce(department_care_network.department_center, pfex_all.department_name)) as drill_down_two,
    pfex_all.visit_date as metric_date,
    pfex_all.tbs_ind as num,
    'sum' as num_calculation,
    pfex_all.survey_key as denom,
    'count' as denom_calculation,
    'percentage' as metric_type,
    'up' as desired_direction,
    'spec_move_through_visit' as metric_id
from
    {{ref('pfex_all')}} as pfex_all
left join {{ref('department_care_network')}} as department_care_network
    on pfex_all.dept_key = department_care_network.dept_key
where
    lower(survey_line_name) = 'specialty care'
    and lower(section_name) = 'moving through your visit'
