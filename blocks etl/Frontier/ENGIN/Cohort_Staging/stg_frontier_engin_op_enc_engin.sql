-- all engin specific visit type encounters
select
    stg_encounter.visit_key,
    stg_encounter.encounter_date,
    stg_encounter.pat_key,
    stg_encounter.appointment_status_id
from {{ ref('stg_encounter') }} as stg_encounter
inner join {{ ref('lookup_frontier_program_visit')}} as lookup_fp_visit
    on stg_encounter.visit_type_id = cast(lookup_fp_visit.id as nvarchar(20))
    and lookup_fp_visit.program = 'engin'
    and lookup_fp_visit.category = 'engin visit'
    and lookup_fp_visit.active_ind = 1
left join {{ ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
    on stg_encounter.visit_key = diagnosis_encounter_all.visit_key
    and diagnosis_encounter_all.visit_diagnosis_ind = 1
    and lower(diagnosis_encounter_all.icd10_code) = 'xxxxbc' -- erroneous encounter--disregard
where
    stg_encounter.appointment_status_id in (1, -- scheduled
                                            2, -- completed
                                            6, --arrived
                                            4 -- no show (for calculating 'no show' metric)
                                            )
    and year(add_months(stg_encounter.encounter_date, 6)) >= 2020 --FY20 and after
    and diagnosis_encounter_all.visit_key is null
