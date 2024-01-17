select
    'Program-Specific: Surgeries' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'surgery_procedure.or_key',
            'surgery_procedure.procedure_seq_num'
        ])
    }} as primary_key,
    initcap(or_procedure_name) as drill_down_one,
    surgery_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_surgery' as metric_id,
    primary_key as num
from
    {{ ref('frontier_engin_encounter_cohort')}} as cohort_enc
    inner join {{ ref('surgery_procedure') }} as surgery_procedure
        on cohort_enc.visit_key = surgery_procedure.visit_key
        --and lower(case_status) = 'completed' --include completed and scheduled surgeries
    inner join {{ ref('lookup_frontier_program_procedures')}} as lookup_fp_procedure
        on surgery_procedure.or_proc_id = cast(lookup_fp_procedure.id as nvarchar(20))
        and lookup_fp_procedure.program = 'engin'
        and lookup_fp_procedure.category = 'surgery'
        and lookup_fp_procedure.active_ind = 1
where
    cohort_enc.surgery_ind = 1
