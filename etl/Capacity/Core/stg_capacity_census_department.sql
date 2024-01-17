with stage as (
    select
        visit_key,
        encounter_key,
        visit_event_key,
        stg_adt_all.dept_key,
        pat_key,
        patient_key,
        dept_enter_date as enter_date,
        dept_exit_date_or_current_date as exit_date_or_current_date,
        case
            when considered_ip_unit = 1
            then stg_department_all.department_id
            -- never overwrite MAIN TRANSPORT events
            when stg_department_all.department_id = 101001032
            -- set non IP units to null to fill in next step
            then stg_department_all.department_id
            else null
        end as stg_census_dept_id,
        sum(
            case
                when considered_ip_unit = 0
                then 0
            else 1 end
        ) over (partition by visit_key order by dept_enter_date) as value_partition
    from
        {{ref('stg_adt_all')}} as stg_adt_all
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.department_id = stg_adt_all.department_id
    where
        department_ind = 1
        and coalesce(hospital_discharge_date, current_date) >= '2014-01-01'
        and hospital_admit_date > '2012-01-01'
),

census_department as (
    select
        visit_key,
        encounter_key,
        visit_event_key,
        pat_key,
        patient_key,
        enter_date,
        exit_date_or_current_date,
        coalesce(
            stg_census_dept_id, first_value(stg_census_dept_id)
        over (partition by visit_key, value_partition
        order by enter_date)
        ) as census_department_id,
        case
            when census_department_id = 101001032
            then 0
            when census_department_id is not null
            then 1
            else 0
        end as census_incl_ind
    from
        stage
)

select
    census_department.visit_key,
    census_department.encounter_key,
    census_department.visit_event_key,
    census_department.pat_key,
    census_department.patient_key,
    fact_department_rollup.dept_key as census_dept_key,
    census_department.enter_date,
    census_department.exit_date_or_current_date,
    fact_department_rollup.dept_id as department_id,
    fact_department_rollup.mstr_dept_grp_unit_key as department_group_key,
    fact_department_rollup.dept_nm as department_name,
    fact_department_rollup.unit_dept_grp_abbr as department_group_name,
    fact_department_rollup.loc_dept_grp_abbr as location_group_name,
    fact_department_rollup.bed_care_dept_grp_abbr as bed_care_group,
    case
        when lower(fact_department_rollup.unit_dept_grp_abbr) = 'cpru'
        then 'PHL IP Cmps'
        else fact_department_rollup.department_center_abbr
    end as department_center_abbr
from
    census_department
    inner join {{source('cdw_analytics','fact_department_rollup')}} as fact_department_rollup
        on fact_department_rollup.dept_id = census_department.census_department_id
        and date(exit_date_or_current_date) = fact_department_rollup.dept_align_dt
where
    census_incl_ind = 1
