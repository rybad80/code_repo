select distinct
    'Program-Specific: General ECHO' as metric_name,
    procedure_billing.visit_key as primary_key,
    procedure_billing.procedure_name as drill_down_one,
    procedure_billing.billing_provider_name as drill_down_two,
    procedure_billing.service_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_heart_valve_gen_echo' as metric_id,
    procedure_billing.visit_key as num
from
    {{ ref('frontier_heart_valve_encounter_cohort')}} as frontier_heart_valve_encounter_cohort
    inner join {{ ref('procedure_billing')}} as procedure_billing
        on frontier_heart_valve_encounter_cohort.mrn = procedure_billing.mrn
where
    lower(procedure_name) like '%echo%'
    and year(add_months(service_date, 6)) >= '2020'
