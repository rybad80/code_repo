/* stg_nursing_dept_cc_p2_ends
capture max dates from the Kronos productive hour or Epic encounter dates to get
end time periods for departments that are now inactive
*/
with
inactive_dept as (
    select
        department_id as old_department_id,
        department_name,
        department_abbr
    from
        {{ ref('stg_department_staffing') }}
    where
        record_status_active_ind = 0
),

get_min_pp_dt_key as (
    select
        min(pp_end_dt_key) as default_early_pp_dt_key,
        min(pp_end_dt) as default_early_pp_dt
    from
        {{ ref('nursing_pay_period') }}
),

get_max_enc_date as (
    select
        max(encounter_all.encounter_date) as max_enc_dt,
        encounter_all.department_id
    from
        {{ ref('encounter_all') }} as encounter_all
        inner join inactive_dept
            on encounter_all.department_id = inactive_dept.old_department_id
   where
        encounter_all.encounter_date < current_date
    group by
        encounter_all.department_id
),

max_enc_dept_pp as (
    select
        get_max_enc_date.department_id,
        get_max_enc_date.max_enc_dt,
        coalesce(nursing_pay_period.pp_end_dt,
            get_min_pp_dt_key.default_early_pp_dt) as enc_pp_end_dt,
        coalesce(nursing_pay_period.pp_end_dt_key,
            get_min_pp_dt_key.default_early_pp_dt_key) as enc_pp_end_dt_key
    from
        get_max_enc_date
        left join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on get_max_enc_date.max_enc_dt
            between nursing_pay_period.pp_start_dt
            and nursing_pay_period.pp_end_dt
        left join get_min_pp_dt_key
            on get_max_enc_date.max_enc_dt < get_min_pp_dt_key.default_early_pp_dt

),

get_max_kronos_date as (
    select
        max(prdctv_time.pp_end_dt_key) as max_prdctv_hrs_dt_key,
        prdctv_time.cost_center_id
    from
        {{ ref('timereport_daily_productive_direct') }}  as prdctv_time
    group by
        prdctv_time.cost_center_id
),

max_kronos_dept_pp as ( /* keep only if before current time, get inactive depts */
    select
        get_max_kronos_date.cost_center_id,
        get_max_kronos_date.max_prdctv_hrs_dt_key,
        nursing_pay_period.pp_end_dt as kronos_pp_end_dt,
        nursing_pay_period.pp_end_dt_key as kronos_pp_end_dt_key,
        dept_cc_hist.department_id
    from
        get_max_kronos_date
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on get_max_kronos_date.max_prdctv_hrs_dt_key
            = nursing_pay_period.pp_end_dt_key
            and nursing_pay_period.prior_pay_period_ind = 1
        -- inner join to get the dept(s) to end per Kronos time keeping end
        inner join {{ ref('stg_department_cost_center_history') }} as dept_cc_hist
            on nursing_pay_period.pp_end_dt_key = dept_cc_hist.pp_end_dt_key
            and get_max_kronos_date.cost_center_id = dept_cc_hist.cost_center_id
        inner join inactive_dept
            on dept_cc_hist.department_id = inactive_dept.old_department_id
),
/*
kronos_latest_info as ( -- CTE if needed for troubleshooting
    select
        cc.cost_center_display,
        cc.cc_active_ind,
        cte.*,
        dept.department_name,
        dept.record_status_active_ind
    from
        max_kronos_dept_pp cte
        inner join {{ ref('stg_department_staffing') }} as dept
            on cte.department_id = dept.department_id
        inner join {{ ref('nursing_cost_center_attributes') }} as cc
            on cte.cost_center_id  = cc.cost_center_id
),
*/

get_either_date as (
    select
        max_enc.enc_pp_end_dt,
        max_enc.enc_pp_end_dt_key,
        round(coalesce(max_enc.department_id, max_kronos_dept_pp.department_id), 0) as department_id,
        max_kronos_dept_pp.kronos_pp_end_dt,
        max_kronos_dept_pp.kronos_pp_end_dt_key,
        max_kronos_dept_pp.cost_center_id
    from
        max_enc_dept_pp as max_enc
	full outer join max_kronos_dept_pp
        on max_enc.department_id = max_kronos_dept_pp.department_id
    where
        max_enc.enc_pp_end_dt < current_date
        or max_kronos_dept_pp.department_id is not null
)

select
    cc.cost_center_display,
    cc.cc_active_ind,
    possible_dates.enc_pp_end_dt,
    possible_dates.enc_pp_end_dt_key,
    possible_dates.department_id,
    possible_dates.kronos_pp_end_dt,
    possible_dates.kronos_pp_end_dt_key,
    possible_dates.cost_center_id,
    dept.department_name,
    dept.record_status_active_ind,
    case
        when enc_pp_end_dt < kronos_pp_end_dt
        then enc_pp_end_dt else kronos_pp_end_dt
    end as earliest_end_pp_dt,
    round(months_between(enc_pp_end_dt, kronos_pp_end_dt), 1) as months_between_periods,
    case
        when enc_pp_end_dt < '01-jan-2012'
        then 'very old dept'
        when enc_pp_end_dt < '01-jan-2020'
        then 'pre-Workday' else ''
    end as even_use_department_txt,
    case
        when kronos_pp_end_dt > '01-jul-2022'
        then 'cc active in FY23 still'
        when kronos_pp_end_dt < '01-jul-2020'
        then 'pre-Workday cc' else ''
    end as cc_date_note,
    case
        when even_use_department_txt = 'very old dept'
        then 0 else 1
    end as build_dept_cc_history_ind
from
    get_either_date as possible_dates
        inner join {{ ref('stg_department_staffing') }} as dept
            on possible_dates.department_id = dept.department_id
        inner join {{ ref('nursing_cost_center_attributes') }} as cc
            on possible_dates.cost_center_id  = cc.cost_center_id
