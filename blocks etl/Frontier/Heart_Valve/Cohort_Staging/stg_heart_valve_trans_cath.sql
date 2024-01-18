select
    sp.visit_key,
    sp.mrn,
    sp.patient_name,
    sp.csn as surgery_csn,
    sp.or_procedure_name,
    sp.surgery_date,
    ea.patient_class
from {{ ref('surgery_procedure') }} as sp
    left join {{ ref('stg_encounter') }} as ea
        on sp.visit_key = ea.visit_key
where
    year(add_months(sp.surgery_date, 6)) > '2020'
    and lower(sp.primary_surgeon) like '%gillespie%'
    and lower(sp.or_procedure_name) = 'transcatheter valve placement'
