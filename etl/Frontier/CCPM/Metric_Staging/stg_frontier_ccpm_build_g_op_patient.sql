select
    'CCPM: Outpatients (Unique)' as metric_name,
    frontier_ccpm_encounter_cohort.visit_key as primary_key,
    frontier_ccpm_encounter_cohort.patient_sub_cohort as drill_down_one,
    frontier_ccpm_encounter_cohort.department_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_ccpm_op_unique' as metric_id,
    frontier_ccpm_encounter_cohort.pat_key as num
from
    {{ ref('frontier_ccpm_encounter_cohort') }} as frontier_ccpm_encounter_cohort
    left join {{ source('cdw', 'fact_reimbursement') }} as fact_reimbursement
        on frontier_ccpm_encounter_cohort.visit_key = fact_reimbursement.visit_key
where
    inpatient_ind = '0'
    and ((
        fact_reimbursement.det_type_key = '1'
        and fact_reimbursement.chrg_tx_id != '0'
        )
        or frontier_ccpm_encounter_cohort.hsp_acct_key != '0')
    and patient_sub_cohort != 'potential ccpm group'
