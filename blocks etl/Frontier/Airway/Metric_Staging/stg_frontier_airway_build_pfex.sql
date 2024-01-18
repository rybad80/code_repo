select
    frontier_airway_encounter_cohort.visit_key,
    case
        when lower(pfex_all.question_id) = 'a1' then 'Ease of Scheduling Top Box Score (Spec)'
        when lower(pfex_all.question_id) = 'b9' then 'Handling of insurance Top Box Score (Spec)'
        when lower(pfex_all.question_id) = 'o15' then 'Staff Worked Together Top Box Score (Spec)'
        when lower(pfex_all.question_id) = 'o4' then 'Likelihood to Recommend Top Box Score (Spec)'
        when lower(pfex_all.question_id) = 'v60' then 'Wait Time Top Box Score (Spec)'
        when lower(pfex_all.section_name) = 'care provider' then 'All Care Providers (Overall Average)'
    else 'CHECK' end as metric_name,
    pfex_all.survey_key as primary_key,
    pfex_all.department_name as drill_down_one,
    initcap(pfex_all.provider_name) as drill_down_two,
    pfex_all.visit_date as metric_date,
    pfex_all.survey_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'percentage' as metric_type,
    'up' as direction,
    case
        when lower(pfex_all.question_id) = 'a1' then 'fp_airway_ease'
        when lower(pfex_all.question_id) = 'b9' then 'fp_airway_handling_ins'
        when lower(pfex_all.question_id) = 'o15' then 'fp_airway_staff'
        when lower(pfex_all.question_id) = 'o4' then 'fp_airway_ltr'
        when lower(pfex_all.question_id) = 'v60' then 'fp_airway_wait'
        when lower(pfex_all.section_name) = 'care provider' then 'fp_airway_overall'
    else 'CHECK' end as metric_id,
    pfex_all.tbs_ind as num
from
    {{ ref('frontier_airway_encounter_cohort')}} as frontier_airway_encounter_cohort
    inner join {{ ref('pfex_all')}} as pfex_all
        on frontier_airway_encounter_cohort.visit_key = pfex_all.visit_key
where not(frontier_airway_encounter_cohort.inpatient_ind = 1
            and frontier_airway_encounter_cohort.airway_inpatient_ind = 0)
