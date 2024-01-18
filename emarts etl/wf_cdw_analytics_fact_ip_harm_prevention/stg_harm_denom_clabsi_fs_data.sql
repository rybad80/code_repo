with lookup as (
    select distinct
        stg_harm_denom_clabsi_lda_lookup.fs_key,
        stg_harm_denom_clabsi_lda_lookup.fs_id
    from
        {{ ref('stg_harm_denom_clabsi_lda_lookup') }} as stg_harm_denom_clabsi_lda_lookup

),

flowsheet_measure_limited as (
    select
        flowsheet_measure.fs_rec_key,
        flowsheet_measure.fs_key,
        flowsheet_measure.fs_temp_key,
        flowsheet_measure.occurance,
        flowsheet_measure.rec_dt
    from
        {{ source('cdw', 'flowsheet_measure') }} as flowsheet_measure
    where
        flowsheet_measure.meas_val is not null
        and flowsheet_measure.occurance is not null
),

flowsheet_record_limited as (
    select
        flowsheet_record.fs_rec_key,
        flowsheet_record.vsi_key
    from
        {{ source('cdw', 'flowsheet_record') }} as flowsheet_record
),

fs_temp_key_limiter as (
    select distinct stg_harm_denom_clabsi_fs_template.fs_temp_key
    from
        {{ ref('stg_harm_denom_clabsi_fs_template') }} as stg_harm_denom_clabsi_fs_template

)

select
    lookup.fs_key,
    lookup.fs_id,
    flowsheet_measure_limited.fs_rec_key,
    flowsheet_measure_limited.occurance,
    flowsheet_measure_limited.rec_dt,
    flowsheet_record_limited.vsi_key
from
    lookup
inner join flowsheet_measure_limited
    on lookup.fs_key = flowsheet_measure_limited.fs_key
inner join fs_temp_key_limiter
    on flowsheet_measure_limited.fs_temp_key = fs_temp_key_limiter.fs_temp_key
inner join flowsheet_record_limited
    on flowsheet_measure_limited.fs_rec_key = flowsheet_record_limited.fs_rec_key
