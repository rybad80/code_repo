with
combine_note_date_and_op as (--region
    select visit_key, mrn
    from {{ ref('stg_fp_rare_lung_ip_hx_note') }}
    group by visit_key, mrn
    union
    select visit_key, mrn
    from {{ ref('stg_fp_rare_lung_build_op') }}
    group by visit_key, mrn
    --end region
),
life_time_stamps as (
    select
        combine_note_date_and_op.mrn,
        max(case when fp_list_ind = 1 then 1 else 0 end) as fp_list_ind,
        max(case when bpd_complex_ind = 1 then 1 else 0 end) as bpd_complex_ind
    from
        combine_note_date_and_op
        left join {{ ref('stg_fp_rare_lung_op_compile_sde_dx') }} as stg_fp_rare_lung_op_compile_sde_dx
            on combine_note_date_and_op.mrn = stg_fp_rare_lung_op_compile_sde_dx.mrn
        left join {{ ref('stg_fp_rare_lung_op_hx_bpd_ph') }} as stg_fp_rare_lung_op_hx_bpd_ph
            on combine_note_date_and_op.mrn = stg_fp_rare_lung_op_hx_bpd_ph.mrn
    group by
        combine_note_date_and_op.mrn
)
select
    combine_note_date_and_op.visit_key,
    combine_note_date_and_op.mrn,
    stg_encounter.encounter_date,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    case
        when (rl_ip_consult_ind = '1' or rl_ip_progress_ind = '1')
            and (life_time_stamps.bpd_complex_ind is null or life_time_stamps.bpd_complex_ind = '0')
            and life_time_stamps.fp_list_ind != '1'
        then 1 else 0 end
    as ip_by_note_only_ind,
    case
        when
            stg_encounter.encounter_date >= date(stg_encounter.hospital_admit_date)
                and (stg_encounter.encounter_date <= date(stg_encounter.hospital_discharge_date)
                    or stg_encounter.encounter_date <= current_date
                    )
        then 1 else 0 end
    as rare_lung_ip_ind,
    rl_ip_consult_ind,
    rl_ip_progress_ind,
    life_time_stamps.bpd_complex_ind,
    life_time_stamps.fp_list_ind,
    stg_fp_rare_lung_build_op.surgical_bx_ind,
    date(stg_encounter.hospital_admit_date) as admit_start_date,
    date(stg_encounter.hospital_discharge_date) as discharge_date,
    case
        when stg_encounter.hospital_discharge_date is null
        then current_date
            else date(stg_encounter.hospital_discharge_date) end
    as potential_discharge_date
from combine_note_date_and_op
    left join life_time_stamps
        on combine_note_date_and_op.mrn = life_time_stamps.mrn
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on combine_note_date_and_op.visit_key = stg_encounter.visit_key
    left join {{ ref('stg_fp_rare_lung_build_op') }} as stg_fp_rare_lung_build_op
        on combine_note_date_and_op.visit_key = stg_fp_rare_lung_build_op.visit_key
    left join {{ ref('stg_fp_rare_lung_ip_hx_note') }} as note_data
        on combine_note_date_and_op.visit_key = note_data.visit_key
group by
    combine_note_date_and_op.visit_key,
    combine_note_date_and_op.mrn,
    stg_encounter.encounter_date,
    note_data.rl_ip_consult_ind,
    note_data.rl_ip_progress_ind,
    life_time_stamps.bpd_complex_ind,
    life_time_stamps.fp_list_ind,
    stg_fp_rare_lung_build_op.surgical_bx_ind,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date
