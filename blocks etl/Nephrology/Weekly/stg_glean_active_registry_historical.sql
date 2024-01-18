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

registry_membership_status_sequence as ( -- identify all registry membership statuses (with start dates)
    --for the glomerular disease registry.
    select
        registry_data_info.pat_key,
        registry_configuration.registry_id,
        registry_configuration.registry_name,
        dim_registry_status.registry_status_nm,
        registry_membership_history.registry_change_tm,
        registry_membership_history.create_dt as registry_record_create_dt,
        row_number() over (partition by registry_data_info.pat_key, registry_configuration.registry_id
                            order by registry_membership_history.registry_change_tm desc) as seq
    from
        {{source('cdw', 'registry_data_info')}} as registry_data_info -- to get pat_key
        inner join {{source('cdw', 'registry_membership_history')}} as registry_membership_history
            on registry_data_info.record_key = registry_membership_history.record_key --registry membership history
        inner join {{source('cdw', 'registry_configuration')}} as registry_configuration
            on registry_membership_history.registry_config_key = registry_configuration.registry_config_key
            -- to filter to registry_id
        inner join {{source('cdw', 'dim_registry_status')}} as dim_registry_status
            on registry_membership_history.dim_registry_status_key = dim_registry_status.dim_registry_status_key
            -- to pull active/inactive
    where
        registry_configuration.registry_id = '100136' -- CHOP GLOMERULAR DISEASE REGISTRY
)
-- for each registry membership status for the glomerular disease registry, identify the start and end date, 
-- and identify patients with a status of 'active' on glomerular disease registry as of each historical date.
select
    registry_membership_status_sequence.pat_key,
    historical_dates.historical_date,
    historical_dates.cutoff_date,
    registry_membership_status_sequence.registry_id,
    registry_membership_status_sequence.registry_name,
    registry_membership_status_sequence.registry_status_nm,
    registry_membership_status_sequence.seq, -- 1 is current value
    registry_membership_status_sequence.registry_change_tm as status_start_date,
    registry_membership_status_end_dates.registry_change_tm as status_end_date,
    registry_membership_status_sequence.registry_record_create_dt as status_start_create_date,
    registry_membership_status_end_dates.registry_record_create_dt as status_end_create_date
from
    registry_membership_status_sequence
    inner join historical_dates
        on 1 = 1
    left join registry_membership_status_sequence as registry_membership_status_end_dates
        on registry_membership_status_sequence.pat_key = registry_membership_status_end_dates.pat_key
        and registry_membership_status_sequence.registry_id = registry_membership_status_end_dates.registry_id
        and registry_membership_status_sequence.seq = (registry_membership_status_end_dates.seq + 1)
where
    date(status_start_date) <= historical_dates.cutoff_date
    and date(status_start_create_date) <= historical_dates.historical_date
    and (date(status_end_date) > historical_dates.cutoff_date  or status_end_date is null)
    and (date(status_end_create_date) > historical_dates.historical_date or status_end_create_date is null)
    and lower(registry_membership_status_sequence.registry_status_nm) = 'active'
