/* stg_nursing_dept_cc_p3_add_start
add on to the max date work to get start time periods for new departments
and set if any
--> granularity:  all Epic department_ids to make history rows for cost center
*/
with
get_min_enc_date as (
    select
        min(encounter_all.encounter_date) as min_enc_dt,
        encounter_all.department_id
    from
        {{ ref('encounter_all') }} as encounter_all
    group by
        encounter_all.department_id
),

inactive_dept_data as (
    select
        dept_cc_end.department_id,
        earliest_end_pp_dt,
        pp_end_dt_key as earliest_end_pp_dt_key,
        build_dept_cc_history_ind,
        even_use_department_txt
    from
        {{ ref('stg_nursing_dept_cc_p2_ends') }} as dept_cc_end
        inner join {{ ref('nursing_pay_period') }} as pp
            on dept_cc_end.earliest_end_pp_dt = pp.pp_end_dt
),

get_min_pp_dt_key as (
    select
        min(pp_end_dt_key) as default_start_pp_dt_key,
        min(pp_end_dt) as default_start_pp_dt
    from
        {{ ref('nursing_pay_period') }}
),

get_max_pp_dt_key as (
    select
        max(pp_end_dt_key) as take_out_to_pp_dt_key
    from
        {{ ref('nursing_pay_period') }}
    where
        nccs_platform_window_ind = 1
),

add_start_date as (
    select
        dept.department_id,
        get_min_enc_date.min_enc_dt,
        nursing_pay_period.pp_end_dt_key as department_start_dt_key,
        case
            when even_use_department_txt = 'pre-Workday'
            then 1 else 0
        end as inactive_pre_workday_department_ind,
        coalesce(inactive_dept_data.earliest_end_pp_dt_key,
            take_out_to_pp_dt_key) as take_to_end_dt_key
    from
        {{ ref('stg_department_staffing') }} as dept
		left join get_min_enc_date
            on dept.department_id = get_min_enc_date.department_id
		left join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on get_min_enc_date.min_enc_dt
            between nursing_pay_period.pp_start_dt
            and nursing_pay_period.pp_end_dt
		left join inactive_dept_data
            on dept.department_id = inactive_dept_data.department_id
		left join get_max_pp_dt_key
            on coalesce(inactive_dept_data.build_dept_cc_history_ind, 1) = 1
)

select
    department_id,
    coalesce(add_start_date.department_start_dt_key,
        get_min_pp_dt_key.default_start_pp_dt_key) as history_start_dt_key,
    add_start_date.min_enc_dt,
    add_start_date.take_to_end_dt_key
from
    add_start_date
    left join get_min_pp_dt_key
        on get_min_pp_dt_key.default_start_pp_dt < current_date
where
    inactive_pre_workday_department_ind = 0 /* ignore the pre-Workday departments */
