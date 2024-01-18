with
clean_ip_stay as (--region
    select
        mrn,
        encounter_date,
        max(case
            when rare_lung_ip_ind = 1
            then 1 else 0 end)
        as rare_lung_ip_ind
    from
        {{ ref('stg_fp_rare_lung_stage_ip') }}
    group by
        mrn,
        encounter_date
    --end region
),
clean_ip_note_only as (--region
    select
        mrn,
        encounter_date,
        min(case
            when ip_by_note_only_ind = 0
            then 0 else 1 end)
        as ip_by_note_only_ind
    from
        {{ ref('stg_fp_rare_lung_stage_ip') }}
    group by
        mrn,
        encounter_date
    --end region
),
build_ip_data as (--region
    select
        stage_ip_data.visit_key,
        stage_ip_data.mrn,
        stage_ip_data.encounter_date,
        year(add_months(stage_ip_data.encounter_date, 6)) as fiscal_year,
        note_data.note_fiscal_year,
        clean_ip_note_only.ip_by_note_only_ind,
        clean_ip_stay.rare_lung_ip_ind,
        max(case when stage_ip_data.rl_ip_consult_ind = 1 then 1 else 0 end) as rl_ip_consult_ind,
        max(case when stage_ip_data.rl_ip_progress_ind = 1 then 1 else 0 end) as rl_ip_progress_ind,
        stage_ip_data.bpd_complex_ind,
        stage_ip_data.fp_list_ind,
        stage_ip_data.surgical_bx_ind,
        stage_ip_data.admit_start_date,
        stage_ip_data.discharge_date,
        stage_ip_data.potential_discharge_date
    from
        {{ ref('stg_fp_rare_lung_stage_ip') }} as stage_ip_data
        left join clean_ip_stay
            on stage_ip_data.mrn = clean_ip_stay.mrn
                and stage_ip_data.encounter_date = clean_ip_stay.encounter_date
        left join clean_ip_note_only
            on stage_ip_data.mrn = clean_ip_note_only.mrn
                and stage_ip_data.encounter_date = clean_ip_note_only.encounter_date
        left join {{ ref('stg_fp_rare_lung_ip_hx_note') }} as note_data
            on stage_ip_data.visit_key = note_data.visit_key
    group by
        stage_ip_data.visit_key,
        stage_ip_data.mrn,
        stage_ip_data.encounter_date,
        note_data.note_fiscal_year,
        clean_ip_note_only.ip_by_note_only_ind,
        clean_ip_stay.rare_lung_ip_ind,
        stage_ip_data.bpd_complex_ind,
        stage_ip_data.fp_list_ind,
        stage_ip_data.surgical_bx_ind,
        stage_ip_data.admit_start_date,
        stage_ip_data.discharge_date,
        stage_ip_data.potential_discharge_date
    --end region
)
select
    *,
    'inpatient' as patient_type
from build_ip_data
