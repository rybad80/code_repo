--3. patient had any of the cpt codes below billed for a procedure during the past 3 years
select
    surgery_procedure.pat_key,
    surgery_procedure.visit_key,
    surgery_procedure.mrn,
    surgery_procedure.encounter_date
from {{ ref('stg_frontier_thyroid_cohort_base_tmp') }} as cohort_base_tmp
inner join {{ ref('surgery_procedure') }} as surgery_procedure
    on cohort_base_tmp.pat_key = surgery_procedure.pat_key
inner join {{ref('lookup_frontier_program_procedures')}} as lookup_frontier_program_procedures
    on lower(surgery_procedure.cpt_code) = cast(
            lookup_frontier_program_procedures.id as nvarchar(20))
    and lower(lookup_frontier_program_procedures.program) = 'thyroid'
    and lower(lookup_frontier_program_procedures.category) in ('surgery', 'surgery dx')
left join {{ ref('stg_frontier_thyroid_dx_hx') }} as dx_hx
    on surgery_procedure.pat_key = dx_hx.pat_key
where
    year(add_months(surgery_procedure.encounter_date, 6)) >= 2020
    and (lookup_frontier_program_procedures.category = 'surgery'
        or (dx_hx.thyroid_cancer_dx_date is not null
            and dx_hx.thyroid_cancer_dx_date <= surgery_procedure.encounter_date)
        )
group by
    surgery_procedure.pat_key,
    surgery_procedure.visit_key,
    surgery_procedure.mrn,
    surgery_procedure.encounter_date
