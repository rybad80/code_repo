select
    stg_harm_denom_clabsi_lda_visit.pat_lda_key,
    stg_harm_denom_clabsi_lda_visit.fs_key,
    stg_harm_denom_clabsi_lda_visit.visit_key,
    stg_harm_denom_clabsi_lda_visit.dict_lda_type_key,
    stg_harm_denom_clabsi_lda_visit.hosp_admit_dt,
    stg_harm_denom_clabsi_lda_visit.hosp_dischrg_dt,
    stg_harm_denom_clabsi_lda_visit.enc_id,
    stg_harm_denom_clabsi_lda_visit.pat_key,
    stg_harm_denom_clabsi_lda_visit.lda_desc,
    stg_harm_denom_clabsi_lda_visit.place_dt,
    stg_harm_denom_clabsi_lda_visit.lda_id,
    stg_harm_denom_clabsi_lda_visit.remove_dt,
    min(stg_harm_denom_clabsi_fs_data.rec_dt) as first_access,
    max(stg_harm_denom_clabsi_fs_data.rec_dt) as last_access
from
    {{ ref('stg_harm_denom_clabsi_fs_data') }} as stg_harm_denom_clabsi_fs_data
inner join {{ ref('stg_harm_denom_clabsi_lda_visit') }} as stg_harm_denom_clabsi_lda_visit
    on stg_harm_denom_clabsi_fs_data.vsi_key = stg_harm_denom_clabsi_lda_visit.vsi_key
        and stg_harm_denom_clabsi_fs_data.occurance = stg_harm_denom_clabsi_lda_visit.occurance
inner join {{ ref('stg_harm_denom_clabsi_lda_lookup') }} as stg_harm_denom_clabsi_lda_lookup
    on stg_harm_denom_clabsi_fs_data.fs_key = stg_harm_denom_clabsi_lda_lookup.fs_key
        and stg_harm_denom_clabsi_lda_visit.dict_lda_type_key = stg_harm_denom_clabsi_lda_lookup.dict_lda_type_key

where
    -- record date after admission date
    stg_harm_denom_clabsi_fs_data.rec_dt >= stg_harm_denom_clabsi_lda_visit.hosp_admit_dt
    and (
        -- patient has not yet been discharged...
        stg_harm_denom_clabsi_lda_visit.hosp_dischrg_dt is null
        -- ...or record date precedes discharge date
        or stg_harm_denom_clabsi_fs_data.rec_dt <= stg_harm_denom_clabsi_lda_visit.hosp_dischrg_dt
    )
    and (
        -- line placed outside CHOP...
        stg_harm_denom_clabsi_lda_visit.place_dt is null
        -- ...or record date if on or after place date
        or stg_harm_denom_clabsi_fs_data.rec_dt >= stg_harm_denom_clabsi_lda_visit.place_dt
    )
    and (
        -- line has not yet been removed...
        stg_harm_denom_clabsi_lda_visit.remove_dt is null
        -- ...or record date is on or prior to removal date
        or stg_harm_denom_clabsi_fs_data.rec_dt <= stg_harm_denom_clabsi_lda_visit.remove_dt
    )
group by
    stg_harm_denom_clabsi_lda_visit.pat_lda_key,
    stg_harm_denom_clabsi_lda_visit.fs_key,
    stg_harm_denom_clabsi_lda_visit.visit_key,
    stg_harm_denom_clabsi_lda_visit.dict_lda_type_key,
    stg_harm_denom_clabsi_lda_visit.hosp_admit_dt,
    stg_harm_denom_clabsi_lda_visit.hosp_dischrg_dt,
    stg_harm_denom_clabsi_lda_visit.enc_id,
    stg_harm_denom_clabsi_lda_visit.pat_key,
    stg_harm_denom_clabsi_lda_visit.lda_desc,
    stg_harm_denom_clabsi_lda_visit.place_dt,
    stg_harm_denom_clabsi_lda_visit.lda_id,
    stg_harm_denom_clabsi_lda_visit.remove_dt
