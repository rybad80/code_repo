select distinct
    'Program-Specific: Procedures' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'procedure_billing.visit_key',
            'procedure_billing.cpt_code'
        ])
    }} as primary_key,
    upper(lookup_fp_procedure.category) as drill_down_one,
    initcap(lookup_fp_procedure.label) as drill_down_two,
    procedure_billing.service_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_thyroid_procedure' as metric_id,
    primary_key as num
from
    {{ ref('frontier_thyroid_encounter_cohort')}} as cohort_enc
    inner join {{ ref('procedure_billing')}}  as procedure_billing
        on cohort_enc.visit_key = procedure_billing.visit_key
    inner join {{ ref('lookup_frontier_program_procedures')}} as lookup_fp_procedure
        on procedure_billing.cpt_code = cast(lookup_fp_procedure.id as nvarchar(20))
        and lower(lookup_fp_procedure.program) = 'thyroid'
        and lower(lookup_fp_procedure.category) not like 'surgery%'
        and lookup_fp_procedure.active_ind = 1
where
    cohort_enc.procedure_visit_ind = 1
