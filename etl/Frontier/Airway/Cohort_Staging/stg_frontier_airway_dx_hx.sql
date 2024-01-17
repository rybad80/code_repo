--patients meet dx criteria
with dea_sub as (
    select
        dea.mrn,
        dea.pat_key,
        dea.encounter_date,
        dea.visit_key
    from {{ ref('diagnosis_encounter_all') }} as dea
    inner join {{ ref('lookup_frontier_program_diagnoses') }} as lookup_dx
        on dea.icd10_code = lookup_dx.lookup_dx_id
        and lookup_dx.program = 'airway'
    where dea.encounter_date >= '2017-07-01'
    group by
        dea.mrn,
        dea.pat_key,
        dea.encounter_date,
        dea.visit_key
),
pb_sub as (
    select
        procedure_billing.mrn,
        procedure_billing.pat_key,
        procedure_billing.service_date,
        procedure_billing.provider_id
    from {{ ref('procedure_billing') }} as procedure_billing
    inner join {{ ref('lookup_frontier_program_diagnoses') }} as lookup_dx
        on procedure_billing.icd10_code = lookup_dx.lookup_dx_id
        and lookup_dx.program = 'airway'
    where procedure_billing.service_date >= '2017-07-01'
    group by
        procedure_billing.mrn,
        procedure_billing.pat_key,
        procedure_billing.service_date,
        procedure_billing.provider_id
),
all_dx_hx as (
    select
        dea.mrn,
        dea.pat_key,
        dea.encounter_date
    from dea_sub as dea
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on dea.visit_key = stg_encounter.visit_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ ref('lookup_frontier_program_providers_all') }} as p
		on provider.prov_id = cast(p.provider_id as nvarchar(20))
        and p.program = 'airway'
    left join {{source('cdw', 'provider_specialty')}} as provider_specialty
		on provider.prov_key = provider_specialty.prov_key
        and provider_specialty.spec_nm in ('OTOLARYNGOLOGY', 'OTORHINOLARYGOLOGY')
    where p.provider_id is not null or provider_specialty.prov_key is not null
    group by
        dea.mrn,
        dea.pat_key,
        dea.encounter_date
    union all
	select
        procedure_billing.mrn,
        procedure_billing.pat_key,
        procedure_billing.service_date
    from pb_sub as procedure_billing
    inner join {{ ref('lookup_frontier_program_providers_all') }} as p
		on lower(procedure_billing.provider_id) = cast(p.provider_id as nvarchar(20))
        and p.program = 'airway'
    group by
        procedure_billing.mrn,
        procedure_billing.pat_key,
        procedure_billing.service_date
)

select mrn,
	pat_key,
	min(encounter_date) as earliest_dx_date
from all_dx_hx
group by mrn,
        pat_key
