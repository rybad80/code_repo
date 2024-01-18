-- This is on encounter level. (Multiple surgeries could be done in one encounter.)
select
    surgery_procedure.visit_key,
    surgery_procedure.pat_key
from
    {{ ref('stg_frontier_engin_cohort_base') }} as cohort_base
    inner join {{ ref('surgery_procedure') }} as surgery_procedure
        on cohort_base.pat_key = surgery_procedure.pat_key
        --and lower(case_status) = 'completed' --include completed and scheduled surgeries
    inner join {{ ref('lookup_frontier_program_procedures')}} as lookup_fp_procedure
        on surgery_procedure.or_proc_id = cast(lookup_fp_procedure.id as nvarchar(20))
        and lookup_fp_procedure.program = 'engin'
        and lookup_fp_procedure.category = 'surgery'
        and lookup_fp_procedure.active_ind = 1
where
    lower(surgery_procedure.primary_surgeon) like '%kennedy,%benjamin%'
    and surgery_procedure.surgery_date >= cohort_base.engin_start_date
group by
    surgery_procedure.visit_key,
    surgery_procedure.pat_key
