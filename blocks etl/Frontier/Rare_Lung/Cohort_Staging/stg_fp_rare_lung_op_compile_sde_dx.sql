with
stg_rl_dx_sde_base as (
    select *, 1 as sde_ind, 0 as dx_ind, 0 as fp_list_ind
    from {{ ref('stg_fp_rare_lung_op_hx_sde') }}
    union
    select *, 0 as sde_ind, 1 as dx_ind, 0 as fp_list_ind
    from {{ ref('stg_fp_rare_lung_op_hx_dx') }}
    union
    select *, 0 as sde_ind, 0 as dx_ind, 1 as fp_list_ind
    from {{ ref('stg_fp_rare_lung_op_fp_clinical_list') }}
    group by
        mrn
),
clean_rl_dx_sde_base as (
    select
        mrn,
        max(case when sde_ind = 1 then 1 else 0 end) as sde_ind,
        max(case when dx_ind = 1 then 1 else 0 end) as dx_ind,
        max(case when fp_list_ind = 1 then 1 else 0 end) as fp_list_ind
    from
        stg_rl_dx_sde_base
    group by
        mrn
)
select
    stg_encounter.visit_key,
    clean_rl_dx_sde_base.mrn,
    clean_rl_dx_sde_base.sde_ind,
    clean_rl_dx_sde_base.dx_ind,
    clean_rl_dx_sde_base.fp_list_ind,
    1 as sde_dx_compile_ind
from
    clean_rl_dx_sde_base
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on clean_rl_dx_sde_base.mrn = stg_encounter.mrn
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_frontier_program_providers_all
        --on stg_encounter.provider_id = cast(
        on provider.prov_id = cast(
            lookup_frontier_program_providers_all.provider_id as nvarchar(20))
        and lookup_frontier_program_providers_all.program = 'rare-lung'
where
    lower(department_name) like '%pulm%'
    and (visit_type_id not in (
            --0,    -- default
            '4151', --  follow up cystic fibrosis
            '4158', --  follow up pcd
            '4107', --  new sleep visit
            '4132', --  php fol up
            '2755'  --  video visit sleep fol up
        )
        or (--visit-type = default & encounter-type = 'care coordination'
            visit_type_id = 0 and encounter_type_id = '160')
        )
    and year(add_months(stg_encounter.encounter_date, 6)) >= 2022
    and stg_encounter.encounter_date < current_date
