with by_specialty as (
    select *, --noqa
        row_number() over(
                partition by specialty_name, as_of_date
                order by slot_begin_time) as row_num,
         'SPECIALTY' as group_by
    from {{ref('stg_tna_valid_grouped_slot_history_rows_deleted')}}
    where
        same_day_slot_ind = 0 --skips same day slots
        and physician_app_psych_visit_ind = 1
),

by_department as (
    select *, --noqa
        row_number() over(
                partition by department_name, as_of_date
                order by slot_begin_time) as row_num,
        'DEPARTMENT' as group_by
    from {{ref('stg_tna_valid_grouped_slot_history_rows_deleted')}}
    where
        same_day_slot_ind = 0 --skips same day slots
        and physician_app_psych_visit_ind = 1
),

by_provider as (
    select *,
        row_number() over(
                partition by provider_name, as_of_date
                order by slot_begin_time) as row_num,
        'PROVIDER' as group_by
    from {{ref('stg_tna_valid_grouped_slot_history_rows_deleted')}}
    where
        same_day_slot_ind = 0 --skips same day slots
        and physician_app_psych_visit_ind = 1
),

by_dept_prov as (
    select *,
        row_number() over(
                partition by department_name, provider_name, as_of_date
                order by slot_begin_time) as row_num,
        'DEPT_PROV' as group_by
    from {{ref('stg_tna_valid_grouped_slot_history_rows_deleted')}}
    where
        same_day_slot_ind = 0 --skips same day slots
        and physician_app_psych_visit_ind = 1
),

by_block as (
    select *,
        row_number() over(
                partition by appt_block_c, department_name, as_of_date
                order by slot_begin_time) as row_num,
        'BLOCK' as group_by
    from {{ref('stg_tna_valid_grouped_slot_history_rows_deleted')}}
    where
        same_day_slot_ind = 0 --skips same day slots
        and physician_app_psych_visit_ind = 1
),

all_outpatient as (
    select
        *
    from
        by_specialty
    where
        row_num < 4

    union all

    select
        *
    from
        by_department
    where
        row_num < 4

    union all

    select
        *
    from
        by_provider
    where
        row_num < 4

    union all

    select
        *
    from
        by_dept_prov
    where
        row_num < 4

    union all

    select
        *
    from
        by_block
    where
        row_num < 4
)

select
    *,
    case
        when row_num = 1 then '1st Next Available'
        when row_num = 2 then '2nd Next Available'
        when row_num = 3 then '3rd Next Available'
    else null
    end as next_available
from
    all_outpatient
