with dev_screening as (

select
    procedure_order_all.visit_key,
    procedure_order_all.pat_key,
    procedure_order_all.proc_key,
    procedure_records.proc_id,
    procedure_records.proc_nm
from
    {{ref('procedure_order_all')}} as procedure_order_all
    inner join {{source('cdw', 'procedure')}} as procedure_records
        on procedure_order_all.proc_key = procedure_records.proc_key
where
	cpt_code like '96110.%'
)

select
    encounter_primary_care.visit_key,
    encounter_primary_care.department_name,
    encounter_primary_care.encounter_date,
    encounter_primary_care.dob,
    encounter_primary_care.csn,
    encounter_primary_care.age_months,
    encounter_primary_care.provider_name,
    encounter_primary_care.dept_key,
    encounter_primary_care.prov_key,
    encounter_primary_care.pat_key,
    dev_screening.pat_key as screening_pat_key,
    encounter_primary_care.well_visit_ind,
    case when dev_screening.visit_key is not null then 1 else 0 end as development_screening_ind
from
    {{ref('encounter_primary_care')}} as encounter_primary_care
    left join dev_screening
        on encounter_primary_care.visit_key = dev_screening.visit_key
where
    encounter_primary_care.age_months between 8.1 and 36 -- screening may occur after 8 months and 1 week
    and encounter_primary_care.well_visit_ind = 1
