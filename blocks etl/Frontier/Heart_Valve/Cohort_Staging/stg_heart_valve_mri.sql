select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    stg_encounter.visit_type as enc_all_visit_type,
    stg_encounter.encounter_type,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    stg_encounter.department_name,
    cardiac_mri.procedure_order_description,
    cardiac_mri.ordering_provider,
    cardiac_mri.order_status,
    cardiac_mri.patient_class,
    cardiac_mri.visit_type as cardiac_mri_visit_type,
    cardiac_mri.mri_start_date,
    cardiac_mri.mri_reading_provider,
    cardiac_mri.sedation_indicator
from {{ ref('cardiac_valve_center') }} as cardiac_valve_center
	inner join {{ ref('cardiac_mri') }} as cardiac_mri
		on cardiac_valve_center.mrn = cardiac_mri.mrn
	inner join {{ ref('stg_encounter') }}  as stg_encounter
		on cardiac_mri.mri_visit_key = stg_encounter.visit_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
where
    year(add_months(stg_encounter.encounter_date, 6)) > 2020
