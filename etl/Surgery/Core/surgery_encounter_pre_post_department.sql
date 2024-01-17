with depts_raw as (
    select
        surgery_encounter.visit_key,
        surgery_encounter.log_key,
        -- true preop exit
        coalesce(
                surgery_encounter_timestamps.in_preop_room_date,
                surgery_encounter_timestamps.in_room_date,
                surgery_encounter_timestamps.anesthesia_start_date
        ) as preop_exit_date,
        -- true postop enter
        coalesce(
            surgery_encounter_timestamps.recovery_exit_date,
            surgery_encounter_timestamps.recovery_complete_date,
            surgery_encounter_timestamps.anesthesia_stop_date,
            surgery_encounter_timestamps.recovery_phase_2_date,
            surgery_encounter_timestamps.out_room_date
        ) as postop_enter_date,
        adt_department.enter_date,
        adt_department.exit_date,
        stg_department_all.department_id,
        floor(
            lag(
                stg_department_all.department_id, 1, null
            ) over(partition by adt_department.visit_key, surgery_encounter.log_key
                    order by adt_department.enter_date
                )
        ) as prev_department_id,
        lag(
            adt_department.enter_date, 1, null
            ) over(partition by adt_department.visit_key, surgery_encounter.log_key
                   order by adt_department.enter_date
        ) as prev_enter_date,
        lag(
            adt_department.exit_date, 1, null
            ) over(partition by adt_department.visit_key, surgery_encounter.log_key
                    order by adt_department.enter_date
        ) as prev_exit_date,
        floor(
            lead(
                stg_department_all.department_id, 1, null
            ) over(partition by adt_department.visit_key, surgery_encounter.log_key
                    order by adt_department.enter_date
                )
        ) as next_department_id,
        lead(
            adt_department.enter_date, 1, null
            ) over(partition by adt_department.visit_key, surgery_encounter.log_key
                    order by adt_department.enter_date
        ) as next_enter_date,
        lead(
            adt_department.exit_date, 1, null
            ) over(partition by adt_department.visit_key, surgery_encounter.log_key
                    order by adt_department.enter_date
        ) as next_exit_date
    from
        {{ ref('surgery_encounter') }} as surgery_encounter
        inner join {{ ref('adt_department') }} as  adt_department
            on adt_department.visit_key = surgery_encounter.visit_key
        inner join
            {{ ref('surgery_encounter_timestamps') }} as surgery_encounter_timestamps
            on surgery_encounter_timestamps.log_key = surgery_encounter.log_key
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.dept_key = adt_department.dept_key
    where
        lower(surgery_encounter.location_group) != 'asc'
),

dept_summary as (
    select
        depts_raw.visit_key,
        depts_raw.log_key,
        depts_raw.department_id,
        depts_raw.preop_exit_date,
        depts_raw.postop_enter_date,
        depts_raw.enter_date,
        depts_raw.exit_date,
        -- preop
        case
            when
                depts_raw.preop_exit_date > depts_raw.enter_date and depts_raw.preop_exit_date <= coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id not in (101001069, 58) -- PERIOP COMPLEX, 6 NORTHWEST
            then depts_raw.department_id
            when
                depts_raw.preop_exit_date > depts_raw.enter_date
                    and depts_raw.preop_exit_date <= depts_raw.exit_date
                    and depts_raw.department_id in (101001069, 58)
            then depts_raw.prev_department_id
            end as preop_department_id,

        case
            when
                depts_raw.preop_exit_date > depts_raw.enter_date and depts_raw.preop_exit_date <= coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id not in (101001069, 58)
            then depts_raw.enter_date
            when
                depts_raw.preop_exit_date > depts_raw.enter_date and depts_raw.preop_exit_date <= coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id in (101001069, 58)
            then depts_raw.prev_enter_date
            end as preop_enter_date,
        -- postop
        case
            when
                depts_raw.postop_enter_date >= depts_raw.enter_date and depts_raw.postop_enter_date < coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id not in (101001069, 58)
            then depts_raw.department_id
            when
                depts_raw.postop_enter_date >= depts_raw.enter_date and depts_raw.postop_enter_date < coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id in (101001069, 58)
            then depts_raw.next_department_id
            end as postop_department_id,

        case
            when
                depts_raw.postop_enter_date >= depts_raw.enter_date and depts_raw.postop_enter_date < coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id not in (101001069, 58)
            then depts_raw.enter_date
            when
                depts_raw.postop_enter_date >= depts_raw.enter_date and depts_raw.postop_enter_date < coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id in (101001069, 58)
            then depts_raw.next_enter_date
            end as adt_postop_enter_date,

        case
            when
                depts_raw.postop_enter_date >= depts_raw.enter_date and depts_raw.postop_enter_date < coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id not in (101001069, 58)
            then depts_raw.exit_date
            when
                depts_raw.postop_enter_date >= depts_raw.enter_date and depts_raw.postop_enter_date < coalesce(
                    depts_raw.exit_date, current_timestamp
                )
                    and depts_raw.department_id in (101001069, 58)
            then depts_raw.next_exit_date
            end as postop_exit_date
    from
        depts_raw
),

adjusted_depts as (
    select
        visit_key,
        log_key,
        max(preop_department_id) as preop_department_id,
        max(preop_enter_date) as preop_enter_date,
        max(preop_exit_date) as preop_exit_date,
        max(postop_department_id) as postop_department_id,
        max(postop_enter_date) as postop_enter_date,
        max(postop_exit_date) as postop_exit_date,
        max(adt_postop_enter_date) as adt_postop_enter_date
    from
        dept_summary
    group by
        visit_key,
        log_key
),

pre_post_dept as (
    select
        adjusted_depts.visit_key,
        adjusted_depts.log_key,
        adjusted_depts.preop_department_id,
        dept_pre.dept_key as preop_dept_key,
        dept_pre.dept_nm as preop_department_name,
        dept_pre.chop_dept_grp_abbr as preop_department_group_abbr,
        adjusted_depts.preop_enter_date,
        adjusted_depts.preop_exit_date,
        adjusted_depts.postop_department_id,
        dept_post.dept_key as postop_dept_key,
        dept_post.dept_nm as postop_department_name,
        dept_post.chop_dept_grp_abbr as postop_department_group_abbr,
        adjusted_depts.postop_enter_date,
        adjusted_depts.postop_exit_date,
        adjusted_depts.adt_postop_enter_date,
        extract(epoch from preop_exit_date - preop_enter_date) / 3600.00 as preop_los_hrs,
        extract(epoch from postop_exit_date - postop_enter_date) / 3600.00 as postop_los_hrs
    from
        adjusted_depts
        left join {{ source('cdw_analytics', 'fact_department_rollup') }} as dept_pre
            on dept_pre.dept_id = adjusted_depts.preop_department_id
            and dept_pre.dept_align_dt = date(adjusted_depts.preop_enter_date)

        left join {{ source('cdw_analytics', 'fact_department_rollup') }} as dept_post
            on dept_post.dept_id = adjusted_depts.postop_department_id
            and dept_post.dept_align_dt = date(adjusted_depts.postop_enter_date)
)

select
    surgery_encounter.log_id,
    surgery_encounter.log_key,
    surgery_encounter.visit_key,
    pre_post_dept.preop_department_id,
    pre_post_dept.preop_dept_key,
    pre_post_dept.preop_department_name,
    pre_post_dept.preop_department_group_abbr,
    pre_post_dept.preop_enter_date,
    pre_post_dept.preop_exit_date,
    pre_post_dept.postop_department_id,
    pre_post_dept.postop_dept_key,
    pre_post_dept.postop_department_name,
    pre_post_dept.postop_department_group_abbr,
    pre_post_dept.postop_enter_date,
    pre_post_dept.postop_exit_date,
	/*If you're looking for times for patients who skipped the PACU post-op, use this field.
    Otherwise use POSTOP_enter_date*/
	pre_post_dept.adt_postop_enter_date,
    pre_post_dept.preop_los_hrs,
    pre_post_dept.postop_los_hrs
from
    {{ ref('surgery_encounter') }} as surgery_encounter
    left join pre_post_dept
        on pre_post_dept.log_key = surgery_encounter.log_key
