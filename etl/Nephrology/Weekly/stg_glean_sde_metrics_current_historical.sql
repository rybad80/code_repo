with historical_dates as (-- historical dates for which historical registry status and metrics will be pulled
    select
        -- list of dates for which metric history will be presented for each patient
        date(dim_date.full_date) as historical_date,
        -- for each historical date, this is the cutoff date for query-able data (as of historical date).
        -- included to ensure data we're pulling now aligns to previously-manually-pulled data on various
        -- historical dates. this alignment may not be important in the future.
        date(historical_date - interval '1 day') as cutoff_date --noqa
    from
        {{ref('dim_date')}} as dim_date
    where
        dim_date.full_date between '2021-02-01' and current_date
        -- can edit this date range as desired. glomerular disease registry data is not available before Jan2021.
        and dim_date.day_of_week = 2 -- limit to one day a week (Monday) for patient-week table
),

sde_max_seq as (
    -- identify max seq_num for each SDE for each patient/date (subquery in GLEAN R report code - moving to CTE)
    select
        smart_data_element_all.pat_key,
        smart_data_element_all.sde_key,
        max(smart_data_element_all.seq_num) as seq
    from
        {{ref('smart_data_element_all')}} as smart_data_element_all
    where
        smart_data_element_all.concept_id in ( -- smart data elements used for defining cohort and for reporting.
                                            'CHOPNEPHRO#004',
                                            'CHOPNEPHRO#007',
                                            'CHOPNEPHRO#008',
                                            'CHOPNEPHRO#010',
                                            'CHOPNEPHRO#052',
                                            'CHOPNEPHRO#053',
                                            'CHOPNEPHRO#055',
                                            'CHOPNEPHRO#056'
                                            )
    group by
        smart_data_element_all.pat_key,
        smart_data_element_all.sde_key
),

sde_current as ( -- current value of each smart data element of interest for each patient, as of date of data pull
    select
        smart_data_element_all.pat_key,
        smart_data_element_all.sde_key,
        smart_data_element_all.concept_id,
        smart_data_element_all.seq_num,
        smart_data_element_all.element_value,
        smart_data_element_all.entered_date,
        'current' as current_historical_cat
    from
        {{ref('smart_data_element_all')}} as smart_data_element_all
        inner join sde_max_seq
            on smart_data_element_all.pat_key = sde_max_seq.pat_key -- identify most recent value per sde per pat
            and smart_data_element_all.sde_key = sde_max_seq.sde_key
            and smart_data_element_all.seq_num = sde_max_seq.seq
),

sde_historical_line_info as (
    -- patients with historical sde values among smart data elements of interest, pull historical value line info. 
    -- This is necessary to correctly identify historical values. 
    select
        sde_current.pat_key,
        sde_current.sde_key,
        sde_current.concept_id,
        cast(smart_data_element_previous_value_list.seq_num as int) as line_location,
        cast(smart_data_element_previous_value_list.sde_prev_val_list as int) as entry_length,
        smart_data_element_previous_value.sde_prev_val_last_upd_dt
    from
        sde_current
    inner join {{source('cdw', 'smart_data_element_previous_value')}} as smart_data_element_previous_value
        on sde_current.sde_key = smart_data_element_previous_value.sde_key
    inner join {{source('cdw', 'smart_data_element_previous_value_list')}} as smart_data_element_previous_value_list --noqa
        on smart_data_element_previous_value.sde_key = smart_data_element_previous_value_list.sde_key
    where
        smart_data_element_previous_value_list.seq_num = smart_data_element_previous_value.sde_prev_val_pointer
    order by
        line_location
),

sde_historical as ( -- for each patient, pull all historical sde values among smart data elements of interest
    select
        smart_data_element_all.pat_key,
        smart_data_element_all.patient_name,
        smart_data_element_all.sde_key,
        sde_historical_line_info.concept_id,
        smart_data_element_previous_value_list.sde_prev_val_list,
        sde_historical_line_info.sde_prev_val_last_upd_dt,
        case when sde_prev_val_last_upd_dt is not null then 1
             else 0
             end as not_null_ind,
        'historical' as current_historical_cat
    from
        {{ref('smart_data_element_all')}} as smart_data_element_all
        inner join {{source('cdw', 'smart_data_element_previous_value_list')}} as smart_data_element_previous_value_list --noqa
            on smart_data_element_all.sde_key = smart_data_element_previous_value_list.sde_key
        left join sde_historical_line_info
            on (smart_data_element_previous_value_list.sde_key = sde_historical_line_info.sde_key
            and smart_data_element_previous_value_list.seq_num
                <= (sde_historical_line_info.line_location + sde_historical_line_info.entry_length)
            and smart_data_element_previous_value_list.seq_num > sde_historical_line_info.line_location)
    where
        not_null_ind = 1
    group by
        smart_data_element_all.pat_key,
        smart_data_element_all.patient_name,
        smart_data_element_all.sde_key,
        sde_historical_line_info.concept_id,
        smart_data_element_previous_value_list.sde_prev_val_list,
        sde_historical_line_info.sde_prev_val_last_upd_dt
),

sde_current_historical as (
    -- union sde current values and historical values,
    -- so that values can be sequenced by patient and start/end dates for each value can be determined.
    select
        sde_current.pat_key,
        sde_current.sde_key,
        sde_current.concept_id,
        sde_current.element_value,
        sde_current.entered_date,
        sde_current.current_historical_cat
    from
        sde_current

    union all

    select
        sde_historical.pat_key,
        sde_historical.sde_key,
        sde_historical.concept_id,
        sde_historical.sde_prev_val_list as element_value,
        sde_historical.sde_prev_val_last_upd_dt as entered_date,
        sde_historical.current_historical_cat
    from
        sde_historical
),

sde_current_historical_sequence as ( -- sequence all sde values (current and historical), by patient and sde.
    select
        sde_current_historical.pat_key,
        sde_current_historical.sde_key,
        sde_current_historical.concept_id,
        sde_current_historical.element_value,
        sde_current_historical.entered_date,
        sde_current_historical.current_historical_cat,
        row_number() over (partition by sde_current_historical.pat_key, sde_current_historical.concept_id
            order by sde_current_historical.entered_date desc) as seq
    from
        sde_current_historical
),

sde_current_historical_dates as ( -- assign start and end time to each sde value
    select
        sde_current_historical_sequence.pat_key,
        sde_current_historical_sequence.sde_key,
        sde_current_historical_sequence.concept_id,
        sde_current_historical_sequence.element_value,
        sde_current_historical_sequence.seq, -- 1 is current value
        sde_current_historical_sequence.entered_date as value_start_date,
        sde_current_historical_end_dates.entered_date as value_end_date
    from
        sde_current_historical_sequence
        left join sde_current_historical_sequence as sde_current_historical_end_dates
            on sde_current_historical_sequence.pat_key = sde_current_historical_end_dates.pat_key
            and sde_current_historical_sequence.concept_id = sde_current_historical_end_dates.concept_id
            and sde_current_historical_sequence.seq = (sde_current_historical_end_dates.seq + 1)
)

select
    sde_current_historical_dates.pat_key,
    historical_dates.historical_date,
    historical_dates.cutoff_date,
    min(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#004'
                then sde_current_historical_dates.value_start_date end) as earliest_phenotype_entered_date,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#004'
                then cast(sde_current_historical_dates.element_value as varchar(100)) else null end) as phenotype,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#007'
                then cast('1840-12-31' as date) + cast(sde_current_historical_dates.element_value as int)
            else null end) as kidney_biopsy_date,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#008'
                then cast(sde_current_historical_dates.element_value as varchar(100))
            else null end) as kidney_biopsy_result,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#010'
                then cast(sde_current_historical_dates.element_value as varchar(100))
            else null end) as genetic_testing_performed,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#052'
                then cast(sde_current_historical_dates.element_value as varchar(100))
            else null end) as imm_rec_rev,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#053'
                then cast(sde_current_historical_dates.element_value as varchar(100))
            else null end) as tb,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#055'
                then cast(sde_current_historical_dates.element_value as varchar(100))
            else null end) as rd_counseling,
    max(case when sde_current_historical_dates.concept_id = 'CHOPNEPHRO#056'
                then cast(sde_current_historical_dates.element_value as varchar(100))
            else null end) as patient_family_education
from
    sde_current_historical_dates
    inner join historical_dates
    on 1 = 1
where
    date(sde_current_historical_dates.value_start_date) <= historical_dates.cutoff_date
    and (date(sde_current_historical_dates.value_end_date) > historical_dates.cutoff_date
        or sde_current_historical_dates.value_end_date is null)
group by
    sde_current_historical_dates.pat_key,
    historical_dates.historical_date,
    historical_dates.cutoff_date
