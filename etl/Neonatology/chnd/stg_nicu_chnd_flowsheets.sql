with flowsheet_raw as (
    --get recorded data from flowsheet_all for temperature, weight and temperature source
    select
        stg_nicu_chnd_timestamps.log_key,
        flowsheet_all.flowsheet_id,
        flowsheet_all.recorded_date,
        flowsheet_all.meas_val,
        case
            when flowsheet_all.flowsheet_id = 6 then (round((flowsheet_all.meas_val_num - 32.0) * (5.0 / 9.0), 1))
            when flowsheet_all.flowsheet_id = 14 then (round(flowsheet_all.meas_val_num * 0.0283495, 3))
        end as fs_val_num,
        case
            when flowsheet_all.recorded_date between
                stg_nicu_chnd_timestamps.pre_procedure_start and stg_nicu_chnd_timestamps.procedure_start_date
            then 1 end as wt_ind,
        case
            when flowsheet_all.recorded_date between
                stg_nicu_chnd_timestamps.pre_temp_window and stg_nicu_chnd_timestamps.time_leave
            then 1 end as pre_ind,
        case
            when flowsheet_all.recorded_date between
                stg_nicu_chnd_timestamps.time_return and stg_nicu_chnd_timestamps.post_temp_window
            then 1 end as post_ind
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        inner join {{ ref('flowsheet_all') }} as flowsheet_all
            on stg_nicu_chnd_timestamps.visit_key = flowsheet_all.visit_key
    where
        flowsheet_all.flowsheet_id in (
            6,          --Temperature
            14,         --Weight
            40000303,   --Temperature Source
            5658        --Secondary Temperature Source
        ) and flowsheet_all.recorded_date >= '2013-01-01'
),

flowsheet_values as (
    --model data for pre-weight, pre/post surgery temperature & post rectal temperature
    select
        stg_nicu_chnd_timestamps.log_key,
        max(pre_weight.fs_val_num) as pre_weight,
        row_number() over (
            partition by stg_nicu_chnd_timestamps.log_key
            order by pre_weight.recorded_date desc
        ) as wt_desc,
        max(case
            when stg_nicu_chnd_timestamps.admitted_after_surgery_ind = 1 then null
            else pre_temp.fs_val_num end
        ) as pre_temp,
        row_number() over (
            partition by stg_nicu_chnd_timestamps.log_key
            order by pre_temp.recorded_date desc
        ) as seq_desc,
        post_temp.flowsheet_id,
        max(case when post_temp.flowsheet_id = 6 then post_temp.fs_val_num end) as post_temp,
        max(case when post_temp.flowsheet_id = 40000303 then post_temp.meas_val end) as post_temp_source,
        row_number() over (
            partition by stg_nicu_chnd_timestamps.log_key,
                post_temp.flowsheet_id
            order by post_temp.recorded_date
        ) as seq_asc,
        max(case when lower(post_temp.meas_val) = 'rectal' then 1 else 0 end) as post_rectal_temp
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        left join flowsheet_raw as pre_weight
            on stg_nicu_chnd_timestamps.log_key = pre_weight.log_key
            and pre_weight.fs_val_num is not null
            and pre_weight.flowsheet_id = 14
            and pre_weight.wt_ind = 1
        left join flowsheet_raw as pre_temp
            on stg_nicu_chnd_timestamps.log_key = pre_temp.log_key
            and pre_temp.flowsheet_id = 6
            and pre_temp.pre_ind = 1
        left join flowsheet_raw as post_temp
            on stg_nicu_chnd_timestamps.log_key = post_temp.log_key
            and post_temp.flowsheet_id in (6, 40000303, 5658)
            and post_temp.post_ind = 1
    group by
        stg_nicu_chnd_timestamps.log_key,
        pre_weight.recorded_date,
        pre_temp.recorded_date,
        post_temp.flowsheet_id,
        post_temp.recorded_date
)

select
    log_key,
    max(case when wt_desc = 1 then pre_weight end) as surgery_weight,
    max(case when seq_desc = 1 then pre_temp end) as pre_temp_value,
    max(case when seq_asc = 1 and flowsheet_id = 6  then post_temp end) as post_temp_value,
    max(case when seq_asc = 1 and flowsheet_id = 40000303 then post_temp_source end) as post_temp_source,
    max(post_rectal_temp) as post_rectal_temp_ind
from
    flowsheet_values
group by
    log_key
