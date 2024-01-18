with
--this code has all updates included - even the fix to the departments
compile_mm_sources_base as (--region
    select visit_key, mrn from {{ ref('stg_minds_matter_dx_hx') }}
    union
    select visit_key, mrn from {{ ref('stg_minds_matter_sde_hx') }}
    union
    select visit_key, mrn from {{ ref('stg_minds_matter_visit_hx') }}
    union
    select visit_key, mrn from {{ ref('stg_minds_matter_reason_hx') }}
    --end region
),
compile_mm_sources as (--region
    select
        compile_mm_sources_base.visit_key,
        compile_mm_sources_base.mrn,
        coalesce(minds_matter_dx_ind, 0) as minds_matter_dx_ind,
        coalesce(minds_matter_sde_ind, 0) as minds_matter_sde_ind,
        coalesce(minds_matter_visit_type_ind, 0) as minds_matter_visit_type_ind,
        coalesce(minds_matter_reason_visit_ind, 0) as minds_matter_reason_visit_ind
    from
        compile_mm_sources_base
        left join {{ ref('stg_minds_matter_dx_hx') }} as stg_minds_matter_dx_hx
            on compile_mm_sources_base.visit_key = stg_minds_matter_dx_hx.visit_key
        left join {{ ref('stg_minds_matter_sde_hx') }} as stg_minds_matter_sde_hx
            on compile_mm_sources_base.visit_key = stg_minds_matter_sde_hx.visit_key
        left join {{ ref('stg_minds_matter_visit_hx') }} as stg_minds_matter_visit_hx
            on compile_mm_sources_base.visit_key = stg_minds_matter_visit_hx.visit_key
        left join {{ ref('stg_minds_matter_reason_hx') }} as stg_minds_matter_reason_hx
            on compile_mm_sources_base.visit_key = stg_minds_matter_reason_hx.visit_key
    --end region
)
select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    stg_encounter.csn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    compile_mm_sources.minds_matter_dx_ind,
    compile_mm_sources.minds_matter_sde_ind,
    compile_mm_sources.minds_matter_visit_type_ind,
    compile_mm_sources.minds_matter_reason_visit_ind,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    case when
        provider.prov_id = cast(
            lookup_frontier_program_providers.provider_id as nvarchar(20))
        then '1' else '0'
    end as minds_matter_patient_ind,
    '0' as minds_matter_pt_occt_ind,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    compile_mm_sources
    left join {{ ref('stg_encounter') }} as stg_encounter
        on compile_mm_sources.visit_key = stg_encounter.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ ref('lookup_frontier_program_providers_all')}} as lookup_frontier_program_providers
        on provider.prov_id = cast(
            lookup_frontier_program_providers.provider_id as nvarchar(20))
                and program = 'minds-matter'
