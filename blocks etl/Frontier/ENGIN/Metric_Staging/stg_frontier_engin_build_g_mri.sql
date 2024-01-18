select
    'Program-Specific: MRI' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'procedure_order_all.cpt_code',
            'frontier_engin_encounter_cohort.visit_key'
        ])
    }} as primary_key,
    frontier_engin_encounter_cohort.department_name as drill_down_one,
    frontier_engin_encounter_cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_mri' as metric_id,
    primary_key as num
from
    {{ ref('frontier_engin_encounter_cohort')}} as frontier_engin_encounter_cohort
    inner join {{ ref('procedure_order_all')}} as procedure_order_all
        on frontier_engin_encounter_cohort.visit_key = procedure_order_all.visit_key
    inner join {{ ref('lookup_frontier_program_procedures')}} as lookup_fp_procedure
        on procedure_order_all.cpt_code = lookup_fp_procedure.id
        and lookup_fp_procedure.program = 'engin'
        and lookup_fp_procedure.category = 'mri'
        and lookup_fp_procedure.active_ind = 1
