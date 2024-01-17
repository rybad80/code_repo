select distinct
    procedure_billing.pat_key,
    procedure_billing.mrn,
    procedure_billing.visit_key,
    procedure_billing.service_date,
    lookup_frontier_program_procedures.category
from {{ ref('stg_frontier_thyroid_cohort_base') }} as cohort_base
inner join {{ref('procedure_billing')}} as procedure_billing
    on cohort_base.pat_key = procedure_billing.pat_key
inner join {{ref('lookup_frontier_program_procedures')}} as lookup_frontier_program_procedures
    on lower(procedure_billing.cpt_code) = cast(
            lookup_frontier_program_procedures.id as nvarchar(20))
    and lookup_frontier_program_procedures.program = 'thyroid'
    and lower(lookup_frontier_program_procedures.category) in ('fnab', 'radioactive iodine')
where
    year(add_months(procedure_billing.service_date, 6)) >= 2020
    and procedure_billing.service_date >= cohort_base.initial_date
