with
cva_ip_cohort as (--region
    select distinct
        mrn
    from {{ ref('frontier_cva_encounter_cohort') }}
    where cva_onco_day_ind = 1
    --end region
)
select
    'CVA: Average Inpatient Length of Stay' as metric_name,
    frontier_cva_encounter_cohort.visit_key as primary_key,
    frontier_cva_encounter_cohort.department_name as drill_down,
    frontier_cva_encounter_cohort.encounter_date as metric_date,
    encounter_inpatient.inpatient_los_days as num,
    frontier_cva_encounter_cohort.visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'fp_cva_los' as metric_id
from
    cva_ip_cohort
    inner join {{ ref('frontier_cva_encounter_cohort') }} as frontier_cva_encounter_cohort
        on cva_ip_cohort.mrn = frontier_cva_encounter_cohort.mrn
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on frontier_cva_encounter_cohort.visit_key = encounter_inpatient.visit_key
