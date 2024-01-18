with havi_hai as (
    select
hai_type,
month_year,
dept_key,
sum(days_value) as num_days
    from
        {{source('cdw', 'hai_population_days')}}
    where
        days_value is not null
        and hai_type = 'HAVI'
    group by hai_type, month_year, dept_key
),
pop_days as (
select
    hai_type,
    month_year,
    coalesce(m.historical_dept_key, h.dept_key) as dept_key,
    sum(days_value) as num_days
from
        {{source('cdw', 'hai_population_days')}} as h
        left join {{source('cdw', 'department')}} as d on d.dept_key = h.dept_key
        left join {{ref('master_harm_prevention_dept_mapping')}} as m
            on m.harm_type = h.hai_type
            and date(h.line_dt_key) between m.start_dt and m.end_dt
            and m.current_dept_key = h.dept_key
            and (m.denominator_only_ind = 1 or m.unit_move_ind = 1)
    where
        days_value is not null
        and hai_type in ('CAUTI', 'CLABSI', 'VAP')
        --exclude the following units
        and d.dept_id not in (
            58, --6 Northwest
            101003001, --KOPH ED
            101001617 --1 East Observation
        )
    group by hai_type, month_year, coalesce(m.historical_dept_key, h.dept_key)
union distinct
    select
'HAVI' as harm_type,
month_year,
dept_key,
num_days
from havi_hai
    union distinct
    select
'HAPI' as harm_type,
month_year,
dept_key,
num_days
from havi_hai
    union distinct
    -- 7/1/18: Only take PIVIE denominator from 2/1/15 since numerator starts here
    select
'PIVIE' as harm_type,
month_year,
dept_key,
num_days
from havi_hai where month_year >= '2015-02'
    union distinct
    select
'VTE' as harm_type,
month_year,
dept_key,
num_days
from havi_hai
    union distinct
    select
'Falls with Injury' as harm_type,
month_year,
dept_key,
num_days
from havi_hai
),
harm_event as (
    select
harm_type,
to_char(harm_event_dt, 'YYYY-MM') as month_year,
dept_key,
sum(denominator_value) as num_days
    from {{ ref('stg_harm_event_all_denominator') }}
    where harm_type <> 'SSI'
    and compare_to_hai_pop_days_ind = 1
    group by harm_type, to_char(harm_event_dt, 'YYYY-MM'), dept_key
)
select
    cast(-1 as bigint) as visit_key,
    date(coalesce(pop_days.month_year, harm_event.month_year) || '-01') as harm_event_dt,
    -1 as pat_key,
    d.dept_key as dept_key,
    cast(coalesce(dept_groups_by_date.mstr_dept_grp_chop_key, dept_groups_imputation.mstr_dept_grp_chop_key, -1) as bigint) as mstr_dept_grp_key,
    coalesce(dept_groups_by_date.chop_dept_grp_nm, dept_groups_imputation.chop_dept_grp_nm, 'Invalid') as dept_grp_nm,
    coalesce(dept_groups_by_date.chop_dept_grp_abbr, dept_groups_imputation.chop_dept_grp_abbr, 'INV') as dept_grp_abbr,
    null as csn,
    -1 as harm_id,
    coalesce(pop_days.hai_type, harm_event.harm_type) as harm_type,
    'FIX FOR HISTORICAL DISCREPANCIES' as numerator_source,
    null as division,
    null as pathogen_code_1,
    null as pathogen_code_2,
    null as pathogen_code_3,
    0 as numerator_value,
    sum(coalesce(pop_days.num_days, 0)) - sum(coalesce(harm_event.num_days, 0)) as denominator_value,
    null as conf_dt,
    null as hosp_admit_dt,
    null as hosp_dischrg_dt,
   0 as compare_to_hai_pop_days_ind
from
    pop_days
    full outer join harm_event
        on harm_event.harm_type = pop_days.hai_type
        and harm_event.month_year = pop_days.month_year
        and pop_days.dept_key = harm_event.dept_key
    inner join {{source('cdw', 'department')}} as d
        on d.dept_key = coalesce(pop_days.dept_key, harm_event.dept_key)
    left join {{ref('stg_harm_dept_grp')}} as dept_groups_by_date
        on dept_groups_by_date.dept_key = d.dept_key
            and date(
                coalesce(pop_days.month_year, harm_event.month_year) || '-01'
            ) = dept_groups_by_date.dept_align_dt
    left join {{ref('stg_harm_dept_grp')}} as dept_groups_imputation
        on dept_groups_imputation.dept_key = d.dept_key
            and dept_groups_imputation.depts_seq_num = 1
where
    date(coalesce(pop_days.month_year, harm_event.month_year) || '-01') between '2010-01-01' and last_day(add_months(current_timestamp, -1))
--    and d.dept_key <> -1
group by
    date(coalesce(pop_days.month_year, harm_event.month_year) || '-01'),
    coalesce(pop_days.hai_type, harm_event.harm_type),
    d.dept_key,
    coalesce(dept_groups_by_date.mstr_dept_grp_chop_key, dept_groups_imputation.mstr_dept_grp_chop_key, -1),
    coalesce(dept_groups_by_date.chop_dept_grp_nm, dept_groups_imputation.chop_dept_grp_nm, 'Invalid'),
    coalesce(dept_groups_by_date.chop_dept_grp_abbr, dept_groups_imputation.chop_dept_grp_abbr, 'INV')
having denominator_value != 0
