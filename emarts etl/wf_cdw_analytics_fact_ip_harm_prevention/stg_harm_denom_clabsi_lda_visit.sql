with flowsheet_lda_group_limited as (
    select
        fs_key,
        dict_lda_type_key
    from
        {{ source('cdw', 'flowsheet_lda_group') }}
    group by
        fs_key,
        dict_lda_type_key
),

vsir_non_null_occurance as (
    select
        vsi_key,
        seq_num as occurance,
        fs_key,
        pat_lda_key
    from
        {{ source('cdw', 'visit_stay_info_rows') }}
    where
        seq_num is not null
),

vsir_limited as (
    select
        vsir_non_null_occurance.vsi_key,
        vsir_non_null_occurance.occurance,
        vsir_non_null_occurance.fs_key,
        vsir_non_null_occurance.pat_lda_key
    from
        vsir_non_null_occurance
    inner join {{ ref('stg_harm_denom_clabsi_fs_template') }} as stg_harm_denom_clabsi_fs_template
        on vsir_non_null_occurance.fs_key = stg_harm_denom_clabsi_fs_template.fs_key
),

lda_visit_limited as (
    select
        patient_lda.pat_lda_key,
        patient_lda.pat_key,
        visit.visit_key,
        patient_lda.lda_id,
        patient_lda.lda_desc,
        patient_lda.lda_site,
        patient_lda.place_dt,
        visit.hosp_admit_dt,
        visit.hosp_dischrg_dt,
        visit.enc_id,
        case
            when date_trunc('day', patient_lda.remove_dt)
                = to_date('11/19/2157', 'MM/DD/YYYY') then null
            else patient_lda.remove_dt
        end as remove_dt
    from
        {{ source('cdw', 'patient_lda') }} as patient_lda
    inner join {{ source('cdw', 'visit') }} as visit

        on patient_lda.pat_key = visit.pat_key
)

select
    lda_visit_limited.pat_lda_key,
    lda_visit_limited.pat_key,
    lda_visit_limited.visit_key,
    lda_visit_limited.lda_id,
    lda_visit_limited.lda_desc,
    lda_visit_limited.lda_site,
    lda_visit_limited.place_dt,
    lda_visit_limited.remove_dt,
    lda_visit_limited.hosp_admit_dt,
    lda_visit_limited.hosp_dischrg_dt,
    lda_visit_limited.enc_id,
    vsir_limited.vsi_key,
    vsir_limited.occurance,
    vsir_limited.fs_key,
    flowsheet_lda_group_limited.dict_lda_type_key
from
    lda_visit_limited
inner join vsir_limited
    on lda_visit_limited.pat_lda_key = vsir_limited.pat_lda_key
inner join flowsheet_lda_group_limited
    on vsir_limited.fs_key = flowsheet_lda_group_limited.fs_key
