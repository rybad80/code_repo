select
    frontier_motility_encounter_cohort.visit_key,
    case
        when lower(question_id) in ('a1') then 'Ease of Scheduling Top Box Score (Spec)'
        when lower(question_id) in ('b9') then 'Handling of insurance Top Box Score (Spec)'
        when lower(question_id) in ('o15') then 'Staff Worked Together Top Box Score (Spec)'
        when lower(question_id) in ('o4') then 'Likelihood to Recommend Top Box Score (Spec)'
        when lower(question_id) in ('v60') then 'Wait Time Top Box Score (Spec)'
        when lower(section_name ) = 'care provider' then 'All Care Providers (Overall Average)'
    else 'CHECK' end as metric_name,
    pfex_all.survey_key as primary_key,
    pfex_all.department_name as drill_down_one,
    pfex_all.visit_date as metric_date,
    pfex_all.survey_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'percentage' as metric_type,
    'up' as direction,
    case
        when lower(question_id) in ('a1') then 'fp_motility_ease'
        when lower(question_id) in ('b9') then 'fp_motility_handling_ins'
        when lower(question_id) in ('o15') then 'fp_motility_staff'
        when lower(question_id) in ('o4') then 'fp_motility_ltr'
        when lower(question_id) in ('v60') then 'fp_motility_wait'
        when lower(section_name ) = 'care provider' then 'fp_motility_overall'
    else 'CHECK' end as metric_id,
    pfex_all.tbs_ind as num
from
    {{ ref('frontier_motility_encounter_cohort') }} as frontier_motility_encounter_cohort
    inner join {{ ref('pfex_all') }} as pfex_all
        on frontier_motility_encounter_cohort.visit_key = pfex_all.visit_key
