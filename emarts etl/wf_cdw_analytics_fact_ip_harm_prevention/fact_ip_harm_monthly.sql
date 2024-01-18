with included_events as (
select
    *,
    case when harm_type != 'UE' or (harm_type = 'UE' and harm_event_dt >= '2022-07-01') then 1 else 0 end as harm_index_incl_ind,
    case when harm_type != 'UE' or (harm_type = 'UE' and harm_event_dt >= '2021-02-01') then 1 else 0 end as indicator_incl_ind
from {{ ref('fact_ip_harm_event_all') }}
)
select
    date_trunc('month', harm_event_dt) as harm_event_dt_month, --should we make explicit a naming convention in fact_ip_harm_event_all?  harm_event_dt in _post vs event_dt in num/denom
    harm_type,
    cast(sum(numerator_value) as numeric(18))as num_of_harm_events,
    cast(sum(denominator_value) as numeric(18)) as num_of_population_days,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from included_events
where indicator_incl_ind = 1
group by harm_type, date_trunc('month', harm_event_dt)
having sum(denominator_value) != 0
union distinct
select
    date_trunc('month', harm_event_dt) as monthyear,
    'Harm Index' as harm_type,
    cast(sum(numerator_value) as numeric(18)) as num_of_harm_events,
    cast(sum(case when harm_type = 'HAVI' then denominator_value else 0 end) as numeric(18)) as num_of_population_days,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from included_events
where harm_index_incl_ind = 1
group by monthyear
having sum(case when harm_type = 'HAVI' then denominator_value else 0 end) != 0
