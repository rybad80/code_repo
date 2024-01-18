with all_dept_date_ranges as (
    select
        adt_department.pat_key,
        adt_department.visit_key,
        adt_department.dept_key,
        adt_department.enter_date as enter_timestamp,
        adt_department.exit_date as exit_timestamp,
        date_trunc('day', adt_department.enter_date)::date as enter_date,
        date_trunc('day', adt_department.exit_date)::date as exit_date
    from
        {{ ref('adt_department') }} as adt_department
        inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
            on adt_department.visit_key = encounter_inpatient.visit_key
),

dim_date_limited as (
    select
        dim_date.*
    from
        {{ ref('dim_date') }} as dim_date
    where
        dim_date.full_date >= '2017-01-01 00:00:00'
        and dim_date.full_date < date_trunc('month', current_date)
),

all_patdays as (
    select
        all_dept_date_ranges.*,
        dim_date_limited.full_date,
        dim_date_limited.full_date::timestamp as full_timestamp_day_start,
        full_timestamp_day_start + interval '86399 seconds' as full_timestamp_day_end,
        case
            when full_timestamp_day_start < all_dept_date_ranges.enter_timestamp
            then all_dept_date_ranges.enter_timestamp
            else full_timestamp_day_start
        end as patday_start,
        case
            when full_timestamp_day_end > all_dept_date_ranges.exit_timestamp
            then all_dept_date_ranges.exit_timestamp
            else full_timestamp_day_end
        end as patday_end,
        date_part('epoch', patday_end) - date_part('epoch', patday_start) as patseconds,
        patseconds / 86400.0 as patdays,
        date_trunc('month', full_date) as year_month
    from
        all_dept_date_ranges
        inner join dim_date_limited
            on dim_date_limited.full_date >= all_dept_date_ranges.enter_date
            and dim_date_limited.full_date <= all_dept_date_ranges.exit_date
),

department_group_patdays as (
    select
        all_patdays.pat_key,
        all_patdays.visit_key,
        all_patdays.year_month,
        all_patdays.patday_start,
        all_patdays.patday_end,
        all_patdays.patdays,
        coalesce(
            fact_department_rollup_summary.unit_dept_grp_abbr,
            'OTHER'
        ) as department_group_name,
        case
            when department_group_name in (
                'PICU',
                'PICU OVF',
                'NICU',
                'CICU',
                'CICU OVF',
                'PCU',
                'SDU'
            ) then 1
            else 0
        end as icu_ind,
        case
            when department_group_name in (
                'ED',
                'EDECU'
            ) then 1
            else 0
        end as exclude_ind,
        case
            when lower(fact_department_rollup_summary.department_center_abbr) like '%kop%' then 'KOPH'
            else 'PHL'
        end as campus_name
    from
        all_patdays
        left join {{ source('cdw_analytics', 'fact_department_rollup_summary') }} as fact_department_rollup_summary
            on all_patdays.dept_key = fact_department_rollup_summary.dept_key
            and all_patdays.full_date >= fact_department_rollup_summary.min_dept_align_dt
            and all_patdays.full_date <= fact_department_rollup_summary.max_dept_align_dt
)

select
    department_group_patdays.year_month,
    department_group_patdays.icu_ind,
    department_group_patdays.exclude_ind,
    department_group_patdays.campus_name,
    department_group_patdays.department_group_name,
    round(sum(department_group_patdays.patdays), 2) as patdays
from
    department_group_patdays
group by
    department_group_patdays.year_month,
    department_group_patdays.icu_ind,
    department_group_patdays.exclude_ind,
    department_group_patdays.campus_name,
    department_group_patdays.department_group_name
