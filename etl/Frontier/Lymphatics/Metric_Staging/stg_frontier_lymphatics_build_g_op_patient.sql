select
    'Lymphatics: Outpatients (Unique)' as metric_name,
    frontier_lymphatics_encounter_cohort.visit_key as primary_key,
    frontier_lymphatics_encounter_cohort.department_name as drill_down,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_lymph_op_unique' as metric_id,
    frontier_lymphatics_encounter_cohort.pat_key as num
from
    {{ ref('frontier_lymphatics_encounter_cohort') }} as frontier_lymphatics_encounter_cohort
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_frontier_program_providers
        on frontier_lymphatics_encounter_cohort.provider_id = cast(
            lookup_frontier_program_providers.provider_id as nvarchar(20))
        and lookup_frontier_program_providers.program = 'lymphatics'
    left join {{ source('cdw', 'fact_reimbursement') }} as fact_reimbursement
        on frontier_lymphatics_encounter_cohort.visit_key = fact_reimbursement.visit_key
where
    inpatient_ind = '0'
    and ((
        fact_reimbursement.det_type_key = '1'
        and fact_reimbursement.chrg_tx_id != '0'
        )
        or frontier_lymphatics_encounter_cohort.hsp_acct_key != '0')
