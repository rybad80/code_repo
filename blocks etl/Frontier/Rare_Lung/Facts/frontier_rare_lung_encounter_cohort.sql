with
rare_lung_base as (
    select
        visit_key,
        mrn
    from
        {{ ref('stg_fp_rare_lung_build_ip') }}
    union
    select
        visit_key,
        mrn
    from
        {{ ref('stg_fp_rare_lung_build_op') }}
    group by
        visit_key,
        mrn
)
select
    rare_lung_base.visit_key,
    rare_lung_base.mrn,
    stg_encounter.csn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    stg_fp_rare_lung_build_ip.note_fiscal_year,
    stg_fp_rare_lung_build_ip.rl_ip_consult_ind,
    stg_fp_rare_lung_build_ip.rl_ip_progress_ind,
    stg_fp_rare_lung_build_ip.ip_by_note_only_ind,
    stg_fp_rare_lung_build_ip.rare_lung_ip_ind,
    encounter_inpatient.inpatient_los_days,
    coalesce(stg_fp_rare_lung_build_op.sde_ind, 0) as sde_ind,
    coalesce(stg_fp_rare_lung_build_op.dx_ind, 0) as dx_ind,
    coalesce(stg_fp_rare_lung_build_op.fp_list_ind,
        stg_fp_rare_lung_build_ip.fp_list_ind) as fp_list_ind,
    coalesce(stg_fp_rare_lung_build_op.bpd_complex_ind,
        stg_fp_rare_lung_build_ip.bpd_complex_ind) as bpd_complex_ind,
    coalesce(stg_fp_rare_lung_build_op.visit_hx_ind, 0) as visit_hx_ind,
    coalesce(stg_fp_rare_lung_build_op.surgical_bx_ind, 0) as surgical_bx_ind,
    stg_encounter.provider_name,
    stg_encounter.provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    stg_fp_rare_lung_build_ip.bpd_complex_ind as ip_bpd_complex_ind,
    stg_fp_rare_lung_build_ip.surgical_bx_ind as ip_surgical_bx_ind,
    stg_fp_rare_lung_build_ip.admit_start_date,
    stg_fp_rare_lung_build_ip.discharge_date,
    stg_encounter.pat_key,
    stg_encounter.appointment_status,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    rare_lung_base
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on rare_lung_base.visit_key = stg_encounter.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on stg_encounter.visit_key = encounter_inpatient.visit_key
    left join {{ ref('stg_fp_rare_lung_build_op') }} as stg_fp_rare_lung_build_op
        on rare_lung_base.visit_key = stg_fp_rare_lung_build_op.visit_key
    left join {{ ref('stg_fp_rare_lung_build_ip') }} as stg_fp_rare_lung_build_ip
        on rare_lung_base.visit_key = stg_fp_rare_lung_build_ip.visit_key
where
    year(add_months(stg_encounter.encounter_date, 6)) >= 2022
