with licensed_bed_range as (
     select
        stg_capacity_staffed_beds.department_id,
        stg_capacity_staffed_beds.eff_dt_key,
        stg_capacity_staffed_beds.staffed_beds,
        stg_capacity_staffed_beds.effective_date,
        lead(stg_capacity_staffed_beds.effective_date) over (
            partition by stg_capacity_staffed_beds.department_id
            order by stg_capacity_staffed_beds.effective_date
        ) as next_bed_count_effective_date
    from
        {{ref('stg_capacity_staffed_beds')}} as stg_capacity_staffed_beds
    where
        stg_capacity_staffed_beds.effective_date <= current_date --noqa: L028
),

bed_count_eff_end as (
    select
        licensed_bed_range.department_id,
        licensed_bed_range.staffed_beds,
        licensed_bed_range.effective_date,
        coalesce(licensed_bed_range.next_bed_count_effective_date, current_date + 1 --noqa: L028
        ) - 1 as end_date
    from
        licensed_bed_range
    --where end_date > '01-jul-2014'
),

in_use as (
    select
        census_dept_key,
        census_date
    from
        {{ref('capacity_ip_hourly_census')}}
    group by
        census_dept_key,
        census_date
)

select
    dim_date.date_key as bed_count_date_key,
    dim_date.full_date as bed_count_date,
    case
        when
            -- set KOPH Beds to 0 until hospital opens
            department_id in (101003002, 101003003, 101003004, 101003005)
            and dim_date.full_date < '2022-01-26'
        then
            0
        when
            -- 8C PICU, 6E CICU OVF and 3W CSH Rehab Overflow are Flex units
            -- and should only have licensed beds for days when in use
            department_id in (101001629, 10021, 101001906)
            and bed_count_date < current_date
            and in_use.census_dept_key is null
        then
            0
        when
            fact_department_rollup.always_count_for_census_ind = 1
        then
            bed_count_eff_end.staffed_beds
        else
            0
    end as licensed_bed_count,
    fact_department_rollup.dept_key,
    bed_count_eff_end.department_id,
    fact_department_rollup.dept_nm as department_name,
    fact_department_rollup.mstr_dept_grp_unit_key as department_group_key,
    fact_department_rollup.unit_dept_grp_abbr as department_group_name,
    fact_department_rollup.bed_care_dept_grp_abbr as bed_care_group,
    fact_department_rollup.intended_use_dept_grp_abbr as intended_use,
    fact_department_rollup.department_center_abbr
from
    bed_count_eff_end
inner join {{ref('dim_date')}} as dim_date
    on dim_date.full_date between bed_count_eff_end.effective_date and bed_count_eff_end.end_date
inner join {{source('cdw_analytics', 'fact_department_rollup')}} as fact_department_rollup
    on fact_department_rollup.dept_id = bed_count_eff_end.department_id
    and dim_date.full_date = fact_department_rollup.dept_align_dt
left join in_use
    on in_use.census_dept_key = fact_department_rollup.dept_key
    and in_use.census_date = fact_department_rollup.dept_align_dt
