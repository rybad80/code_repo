with depts_raw as (
    --region
    select
        fact_periop.visit_key,
        fact_periop.log_key,
        -- true preop exit
        coalesce(periop_times.in_preop_room, periop_times.in_room, periop_times.anes_sed_start) as preop_exit_dt,
        -- true postop enter
        coalesce(
            periop_times.trans_disch,
            periop_times.recovery_complete,
            periop_times.anes_sed_stop,
            periop_times.phase_ii,
            periop_times.out_room
        ) as postop_enter_dt,
        pat_flow.enter_dt,
        pat_flow.exit_dt,
        pat_flow.dept_id,

        floor(
            lag(
                pat_flow.dept_id, 1, null
            ) over(partition by pat_flow.visit_key, fact_periop.log_key order by pat_flow.enter_dt)
        ) as prev_dept_id,
        lag(
            pat_flow.enter_dt, 1, null
        ) over(partition by pat_flow.visit_key, fact_periop.log_key order by pat_flow.enter_dt) as prev_enter_dt,
        lag(
            pat_flow.exit_dt, 1, null
        ) over(partition by pat_flow.visit_key, fact_periop.log_key order by pat_flow.enter_dt) as prev_exit_dt,

        floor(
            lead(
                pat_flow.dept_id, 1, null
            ) over(partition by pat_flow.visit_key, fact_periop.log_key order by pat_flow.enter_dt)
        ) as next_dept_id,
        lead(
            pat_flow.enter_dt, 1, null
        ) over(partition by pat_flow.visit_key, fact_periop.log_key order by pat_flow.enter_dt) as next_enter_dt,
        lead(
            pat_flow.exit_dt, 1, null
        ) over(partition by pat_flow.visit_key, fact_periop.log_key order by pat_flow.enter_dt) as next_exit_dt

    from {{ ref('fact_periop') }} as fact_periop
        inner join {{ ref('fact_patient_flow') }} as  pat_flow
            on pat_flow.visit_key = fact_periop.visit_key
        inner join
            {{ ref('fact_periop_timestamps') }} as periop_times on periop_times.log_key = fact_periop.log_key

    where fact_periop.asc_ind = 0
),

dept_summary as (
    --region
    select
        visit_key,
        log_key,
        dept_id,
        preop_exit_dt,
        postop_enter_dt,
        enter_dt,
        exit_dt,

        -- preop
        case when preop_exit_dt > enter_dt and preop_exit_dt <= coalesce(exit_dt, current_timestamp)
                    and dept_id not in (101001069, 58) -- PERIOP COMPLEX, 6 NORTHWEST
            then dept_id
            when preop_exit_dt > enter_dt and preop_exit_dt <= exit_dt
                    and dept_id in (101001069, 58)
            then prev_dept_id
            end as preop_dept_id,

        case when preop_exit_dt > enter_dt and preop_exit_dt <= coalesce(exit_dt, current_timestamp)
                    and dept_id not in (101001069, 58)
            then enter_dt
            when preop_exit_dt > enter_dt and preop_exit_dt <= coalesce(exit_dt, current_timestamp)
                    and dept_id in (101001069, 58)
            then prev_enter_dt
            end as preop_enter_dt,

        -- postop       
        case when postop_enter_dt >= enter_dt and postop_enter_dt < coalesce(exit_dt, current_timestamp)
                    and dept_id not in (101001069, 58)
            then dept_id
            when postop_enter_dt >= enter_dt and postop_enter_dt < coalesce(exit_dt, current_timestamp)
                    and dept_id in (101001069, 58)
            then next_dept_id
            end as postop_dept_id,

        case when postop_enter_dt >= enter_dt and postop_enter_dt < coalesce(exit_dt, current_timestamp)
                    and dept_id not in (101001069, 58)
            then enter_dt
            when postop_enter_dt >= enter_dt and postop_enter_dt < coalesce(exit_dt, current_timestamp)
                    and dept_id in (101001069, 58)
            then next_enter_dt
            end as adt_postop_enter_dt,

        case when postop_enter_dt >= enter_dt and postop_enter_dt < coalesce(exit_dt, current_timestamp)
                    and dept_id not in (101001069, 58)
            then exit_dt
            when postop_enter_dt >= enter_dt and postop_enter_dt < coalesce(exit_dt, current_timestamp)
                    and dept_id in (101001069, 58)
            then next_exit_dt
            end as postop_exit_dt

    from depts_raw
    --endregion
),

adjusted_depts as (
--region
select
    visit_key,
    log_key,

    max(preop_dept_id) as preop_dept_id,
    max(preop_enter_dt) as preop_enter_dt,
    max(preop_exit_dt) as preop_exit_dt,

    max(postop_dept_id) as postop_dept_id,
    max(postop_enter_dt) as postop_enter_dt,
    max(postop_exit_dt) as postop_exit_dt,

	max(adt_postop_enter_dt) as adt_postop_enter_dt

from dept_summary

group by visit_key, log_key
--endregion
), pre_post_dept as (
--region
select
    adjusted_depts.visit_key,
    adjusted_depts.log_key,

    adjusted_depts.preop_dept_id,
    dept_pre.dept_key as preop_dept_key,
    dept_pre.dept_nm as preop_dept_nm,
    dept_pre.chop_dept_grp_abbr as preop_dept_grp_abbr,
    adjusted_depts.preop_enter_dt,
    adjusted_depts.preop_exit_dt,

    adjusted_depts.postop_dept_id,
    dept_post.dept_key as postop_dept_key,
    dept_post.dept_nm as postop_dept_nm,
    dept_post.chop_dept_grp_abbr as postop_dept_grp_abbr,
    adjusted_depts.postop_enter_dt,
    adjusted_depts.postop_exit_dt,

	adjusted_depts.adt_postop_enter_dt,

    extract(epoch from preop_exit_dt - preop_enter_dt) / 3600.00 as preop_los_hr,
    extract(epoch from postop_exit_dt - postop_enter_dt) / 3600.00 as postop_los_hr


from adjusted_depts
    left join {{ source('cdw_analytics', 'fact_department_rollup') }} as dept_pre
        on dept_pre.dept_id = adjusted_depts.preop_dept_id
        and dept_pre.dept_align_dt = date(adjusted_depts.preop_enter_dt)

    left join {{ source('cdw_analytics', 'fact_department_rollup') }} as dept_post
        on dept_post.dept_id = adjusted_depts.postop_dept_id
        and dept_post.dept_align_dt = date(adjusted_depts.postop_enter_dt)
--endregion
)

select
    fact_periop.log_id,
    fact_periop.log_key,
    fact_periop.visit_key,

    pre_post_dept.preop_dept_id,
    pre_post_dept.preop_dept_key,
    pre_post_dept.preop_dept_nm,
    pre_post_dept.preop_dept_grp_abbr,
    pre_post_dept.preop_enter_dt,
    pre_post_dept.preop_exit_dt,

    pre_post_dept.postop_dept_id,
    pre_post_dept.postop_dept_key,
    pre_post_dept.postop_dept_nm,
    pre_post_dept.postop_dept_grp_abbr,
    pre_post_dept.postop_enter_dt,
    pre_post_dept.postop_exit_dt,

	/*If you're looking for times for patients who skipped the PACU post-op, use this field.
    Otherwise use POSTOP_ENTER_DT*/
	pre_post_dept.adt_postop_enter_dt,

    pre_post_dept.preop_los_hr,
    pre_post_dept.postop_los_hr

from {{ ref('fact_periop') }} as fact_periop
    left join pre_post_dept on pre_post_dept.log_key = fact_periop.log_key
