with included_events as (
select
    *,
    case when harm_type != 'UE' or (harm_type = 'UE' and harm_event_dt >= '2022-07-01') then 1 else 0 end as harm_index_incl_ind,
    case when harm_type != 'UE' or (harm_type = 'UE' and harm_event_dt >= '2021-02-01') then 1 else 0 end as indicator_incl_ind
from {{ ref('fact_ip_harm_event_all') }}
),
combined as (
select
    date_trunc('month', h.harm_event_dt) as harm_event_dt_month,
    h.harm_type,
    cast(coalesce(h.mstr_dept_grp_key, -1) as bigint) as mstr_dept_grp_key,
    coalesce(h.division, 'N/A') as division,
    dept_grp_nm,
    dept_grp_abbr,
    cast(sum(h.numerator_value) as numeric(18)) as num_of_harm_events,
    cast(sum(h.denominator_value) as numeric(18)) as num_of_population_days
from
    included_events as h
    left join {{ref('stg_harm_dept_grp')}} as dept_groups_by_date
        on dept_groups_by_date.dept_key = h.dept_key
        and dept_groups_by_date.dept_align_dt = h.harm_event_dt
    left join {{ref('stg_harm_dept_grp')}} as dept_groups_imputation
        on dept_groups_imputation.dept_key = h.dept_key
        and dept_groups_imputation.depts_seq_num = 1
where
    h.harm_type != 'SSI'
    and h.indicator_incl_ind = 1
group by
    date_trunc('month', h.harm_event_dt),
    h.harm_type,
    coalesce(h.division, 'N/A'),
    h.mstr_dept_grp_key,
    h.dept_grp_nm,
    h.dept_grp_abbr
having
    sum(h.numerator_value) != 0
    or sum(h.denominator_value) != 0
union distinct
select
    date_trunc('month', h.harm_event_dt) as harm_event_dt_month,
    h.harm_type,
    cast(0 as bigint) as mstr_dept_grp_key,
    coalesce(h.division, 'N/A') as division,
    'N/A' as dept_grp_nm,
    'N/A' as dept_grp_abbr,
    cast(sum(h.numerator_value) as numeric(18)) as num_of_harm_events,
    cast(sum(h.denominator_value) as numeric(18)) as num_of_population_days
from included_events as h
where h.harm_type = 'SSI'
and h.indicator_incl_ind = 1
group by
    date_trunc('month', h.harm_event_dt),
    h.harm_type,
    coalesce(h.division, 'N/A')
union distinct
select
    date_trunc('month', h.harm_event_dt) as monthyear,
    'Harm Index' as harm_type,
    cast(coalesce(h.mstr_dept_grp_key, 0) as bigint) as mstr_dept_grp_key,
    coalesce(h.division, 'N/A') as division,
    h.dept_grp_nm,
    h.dept_grp_abbr,
    cast(sum(h.numerator_value) as numeric(18)) as num_of_harm_events,
    cast(sum(case when h.harm_type = 'HAVI' then h.denominator_value else 0 end) as numeric(18)) as num_of_population_days
from
    included_events as h
where
    h.harm_index_incl_ind = 1
group by
    date_trunc('month', h.harm_event_dt),
    coalesce(h.division, 'N/A'),
    h.mstr_dept_grp_key,
    h.dept_grp_nm,
    h.dept_grp_abbr
)
select
    harm_event_dt_month,
    harm_type,
    mstr_dept_grp_key,
    division,
    dept_grp_nm,
    dept_grp_abbr,
    num_of_harm_events as num_of_harm_events,
    num_of_population_days as num_of_population_days,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from
    combined
