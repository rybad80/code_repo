with
compile_cohort as (--region
    select visit_key, mrn
    from {{ ref('stg_fp_rare_lung_op_hx_visit') }}
    union all
    select visit_key, mrn
    from {{ ref('stg_fp_rare_lung_op_hx_bx') }}
    union all
    select visit_key, mrn
    from {{ ref('stg_fp_rare_lung_op_hx_bpd_ph') }}
    union all
    select visit_key, mrn
    from {{ ref('stg_fp_rare_lung_op_compile_sde_dx') }}
    group by
        visit_key,
        mrn
    --end region
),
build_cohort as (--region
    select
        compile_cohort.visit_key,
        compile_cohort.mrn,
        coalesce(sde_dx_hx.sde_ind, 0) as sde_ind,
        coalesce(sde_dx_hx.dx_ind, 0) as dx_ind,
        coalesce(sde_dx_hx.fp_list_ind, 0) as fp_list_ind, --new line
        coalesce(sde_dx_hx.sde_dx_compile_ind, 0) as sde_dx_compile_ind,
        coalesce(bpd_ph_sub_enc.bpd_complex_ind, 0) as bpd_complex_ind,
        coalesce(visit_hx_enc.visit_hx_ind, 0) as visit_hx_ind,
        coalesce(lung_bx_hx_enc.surgical_bx_ind, 0) as surgical_bx_ind
    from
        compile_cohort
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on compile_cohort.visit_key = stg_encounter.visit_key
        left join {{ ref('stg_fp_rare_lung_op_hx_visit') }} as visit_hx_enc
            on stg_encounter.visit_key = visit_hx_enc.visit_key
        left join {{ ref('stg_fp_rare_lung_op_hx_bpd_ph') }} as bpd_ph_sub_enc
            on stg_encounter.visit_key = bpd_ph_sub_enc.visit_key
        left join {{ ref('stg_fp_rare_lung_op_compile_sde_dx') }} as sde_dx_hx
            on stg_encounter.mrn = sde_dx_hx.mrn
        left join {{ ref('stg_fp_rare_lung_op_hx_bx') }} as lung_bx_hx_enc
            on stg_encounter.visit_key = lung_bx_hx_enc.visit_key

    --end region
)
select
    visit_key,
    mrn,
    sde_ind,
    dx_ind,
    fp_list_ind, --new line
    sde_dx_compile_ind,
    bpd_complex_ind,
    visit_hx_ind,
    surgical_bx_ind,
    'outpatient' as patient_type
from build_cohort
group by
    visit_key,
    mrn,
    sde_ind,
    dx_ind,
    fp_list_ind, --new line
    sde_dx_compile_ind,
    bpd_complex_ind,
    visit_hx_ind,
    surgical_bx_ind
having sde_dx_compile_ind
        + bpd_complex_ind
        + visit_hx_ind > 0
        --surgical_bx_ind --biopsy alone currently not considered sufficient rare lung cohort criteria
