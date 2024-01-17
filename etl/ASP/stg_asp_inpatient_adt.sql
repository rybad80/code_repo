-- purpose: get record of inpatient departments / services for patient days
-- granularity: one row per department per day
with adt_raw as (
    select
        adt_department.visit_key,
        adt_department.visit_event_key,
        master_date.full_dt as cohort_date,
        case
            when adt_department.initial_service = 'NOT APPLICABLE' then 'Unknown'
            else adt_department.initial_service
        end as adt_service,
        case
            when lower(adt_department.initial_service) like '%bone marrow transplant%'
            then 1 else 0
        end as bmt_ind,
        adt_department.department_group_name,
        adt_department.department_center_abbr

    from
        {{ref('adt_department')}}                   as adt_department
        inner join {{ref('stg_adt_all')}}           as stg_adt_all
            on adt_department.visit_key = stg_adt_all.visit_key
        inner join {{source('cdw', 'master_date')}} as master_date
            on
                master_date.full_dt between date(adt_department.enter_date) and coalesce(
                    date(adt_department.exit_date), current_date
                )

    where
        master_date.full_dt >= '2011-07-01'
        -- include inpatient patient days based on both inpatient filters
        and adt_department.intended_use_group = 'Inpatient'
        and stg_adt_all.considered_ip_unit = 1
),

adt_all as (
    select
        visit_key,
        visit_event_key,
        cohort_date,
        adt_service,
        department_group_name,
        department_center_abbr,
        -- unique identifier of patient, location, date
        {{
            dbt_utils.surrogate_key([
                'visit_key',
                'visit_event_key',
                'department_group_name',
                'cohort_date'
            ])
        }} as visit_event_date_key,
        bmt_ind

    from
        adt_raw

    group by
        visit_key,
        visit_event_key,
        cohort_date,
        adt_service,
        department_group_name,
        department_center_abbr,
        visit_event_date_key,
        bmt_ind
)

select
    *
from
    adt_all
