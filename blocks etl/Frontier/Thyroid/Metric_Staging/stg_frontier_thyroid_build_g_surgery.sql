select
    'Program-Specific: Surgeries' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'surgery_procedure.visit_key',
            'surgery_procedure.log_id'
        ])
    }} as primary_key,
    initcap(lookup_fp_procedure.label) as drill_down_one,
    surgery_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_thyroid_surgery' as metric_id,
    primary_key as num
from
    {{ ref('frontier_thyroid_encounter_cohort')}} as cohort_enc
    inner join {{ ref('surgery_procedure') }} as surgery_procedure
        on cohort_enc.visit_key = surgery_procedure.visit_key
        and lower(case_status) = 'completed'
    inner join {{ ref('lookup_frontier_program_procedures')}} as lookup_fp_procedure
        on surgery_procedure.cpt_code = cast(lookup_fp_procedure.id as nvarchar(20))
        and lower(lookup_fp_procedure.program) = 'thyroid'
        and lower(lookup_fp_procedure.category) in ('surgery', 'surgery dx')
        and lookup_fp_procedure.active_ind = 1
where
    cohort_enc.surgery_visit_ind = 1
    and (lookup_fp_procedure.category ='surgery'
        or (thyroid_cancer_dx_date is not null and thyroid_cancer_dx_date <= surgery_procedure.encounter_date))
