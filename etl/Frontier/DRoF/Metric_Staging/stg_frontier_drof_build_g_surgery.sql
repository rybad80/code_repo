select
    'Program-Specific: Surgeries' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'surgery_procedure.or_key'
        ])
    }} as primary_key,
    cohort_enc.sub_cohort as drill_down_one,
    surgery_procedure.service as drill_down_two,
    surgery_procedure.surgery_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_drof_surgery' as metric_id,
    primary_key as num
from
    {{ ref('frontier_drof_encounter_cohort')}} as cohort_enc
    inner join {{ ref('surgery_procedure') }} as surgery_procedure
        on cohort_enc.visit_key = surgery_procedure.visit_key
        and lower(surgery_procedure.case_status) = 'completed'
        and cohort_enc.drof_sub_cohort_ind = 1
        and cohort_enc.inpatient_ind = 1
