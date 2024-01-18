select
    stg_encounter.visit_key,
    cardiac_valve_center.mrn,
    stg_encounter.patient_name,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    stg_encounter.encounter_date,
    stg_encounter.visit_type,
    stg_encounter.encounter_type,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    stg_encounter.department_name,
    procedure_order_all.procedure_name,
    procedure_order_all.cpt_code,
    procedure_order_all.procedure_group_name,
    procedure_order_all.billing_service_date,
    procedure_order_all.source_summary,
    procedure_order_all.billing_department_name
from {{ ref('cardiac_valve_center') }} as cardiac_valve_center
	inner join {{ ref('procedure_order_all') }} as procedure_order_all
		on cardiac_valve_center.mrn = procedure_order_all.mrn
	inner join {{ ref('stg_encounter') }}  as stg_encounter
		on procedure_order_all.visit_key = stg_encounter.visit_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
where
	lower(procedure_name) like 'cta %'
	and year(add_months(stg_encounter.encounter_date, 6)) > '2020'
