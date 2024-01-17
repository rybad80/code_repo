with access_all as (

        with access_raw as (

            select pat.pat_mrn_id,
                fs.pat_key,
                pat.full_nm,
                date(fs.recorded_date) - 1
                    as rec_dt,
                v.visit_key,
                v.hosp_admit_dt,
                fs.meas_val
                    as cum_cl_access

            from {{ ref('flowsheet_all') }} as fs
                inner join {{ source('cdw', 'patient') }} as pat on pat.pat_key = fs.pat_key
                inner join {{ source('cdw', 'visit') }} as v on v.visit_key = fs.visit_key

            where time(fs.recorded_date) <= '06:00:00'
                and fs.recorded_date >= '2016-07-01'
                and ((fs.flowsheet_id = '40060070' --manually calculated flowsheet row up through 9/15/2020
                        and fs.recorded_date <= '2020-09-15')
                    or (fs.flowsheet_id = '40060170' --automatically calculated flowsheet row after 9/15/2020
                        and fs.recorded_date > '2020-09-15'))

            union

            select pat.pat_mrn_id,
                fs.pat_key,
                pat.full_nm,
                date(fs.recorded_date)
                    as rec_dt,
                v.visit_key,
                v.hosp_admit_dt,
                fs.meas_val
                    as cum_cl_access

            from {{ ref('flowsheet_all') }} as fs
                inner join {{ source('cdw', 'patient') }} as pat on pat.pat_key = fs.pat_key
                inner join {{ source('cdw', 'visit') }} as v on v.visit_key = fs.visit_key

            where time(fs.recorded_date) > '06:00:00'
                and fs.recorded_date >= '2016-07-01'
                and ((fs.flowsheet_id = '40060070' --manually calculated flowsheet row up through 9/15/2020
                        and fs.recorded_date <= '2020-09-15')
                    or (fs.flowsheet_id = '40060170' --automatically calculated flowsheet row after 9/15/2020
                        and fs.recorded_date > '2020-09-15'))

        )

    select pat_mrn_id,
        pat_key,
        full_nm,
        visit_key,
        rec_dt,
        hosp_admit_dt,
        max(cast(cum_cl_access as int))
            as cum_cl_access

    from access_raw

    group by 1, 2, 3, 4, 5, 6

),
--select *
--from access_all
--order by visit_key, rec_dt
--;

transaction_raw as (
--region raw transaction
select  v_enter.visit_key,
v_enter.eff_event_dt as enter_dt,
dept.dept_id,
dept.dept_nm,
vai.hosp_disch_dt,
dict_svc.dict_nm as adt_service,
dept_group.chop_dept_grp_nm as dept_grp_nm,
dept_group.chop_dept_grp_abbr as dept_grp_abbr,
lag(
    dept_group.chop_dept_grp_nm
) over(partition by v_enter.visit_key order by v_enter.eff_event_dt) as prev_dept_group_nm,
case
    when dept_group.chop_dept_grp_nm != prev_dept_group_nm or prev_dept_group_nm is null then 1 else 0
end as new_dept_group_ind

from
{{ source('cdw', 'visit_addl_info') }} as vai
inner join {{ source('cdw', 'visit_event') }} as v_enter on v_enter.visit_key = vai.visit_key
inner join {{ source('cdw', 'cdw_dictionary') }} as dict_enter on dict_enter.dict_key = v_enter.dict_adt_event_key
inner join
    {{ source('cdw', 'cdw_dictionary') }} as dict_status on dict_status.dict_key = v_enter.dict_event_subtype_key
inner join {{ source('cdw', 'department') }} as dept on dept.dept_key = v_enter.dept_key
inner join {{ source('cdw_analytics','fact_department_rollup_summary') }} as dept_group
    on dept.dept_key = dept_group.dept_key
    and v_enter.eff_event_dt between dept_group.min_dept_align_dt and dept_group.max_dept_align_dt
inner join {{ source('cdw', 'cdw_dictionary') }} as dict_svc on dict_svc.dict_key = v_enter.dict_pat_svc_key
where
dict_enter.src_id in (1, 3)
and dict_status.dict_nm != 'Canceled'

--Pulls only visit events for a specific time period            
and vai.hosp_admit_dt > '01/01/2016'
--endregion
), next_dept as (
--region department exit creation
select visit_key,
enter_dt, lead(enter_dt, 1, hosp_disch_dt)
over(partition by visit_key order by enter_dt) as dept_exit_dt,
lead(dept_grp_abbr, 1, null) over(partition by visit_key order by enter_dt) as next_dept_abbr

from transaction_raw

where new_dept_group_ind = 1
--endregion
), departments as (
--region final department enter and exit
select
transaction_raw.visit_key,
vis.hosp_admit_dt,
vis.hosp_dischrg_dt,
transaction_raw.dept_grp_abbr,
transaction_raw.enter_dt,
--next_dept.dept_exit_dt as exit_dt,
coalesce(next_dept.dept_exit_dt, now()) as exit_dt,
round(((extract(epoch from next_dept.dept_exit_dt  - transaction_raw.enter_dt)) / 60.0) / 60.0, 2) as los_hrs,
round(
    (((extract(epoch from next_dept.dept_exit_dt  - transaction_raw.enter_dt)) / 60.0) / 60.0) / 24.0, 2
) as los_days

from transaction_raw
inner join next_dept on next_dept.visit_key = transaction_raw.visit_key
and transaction_raw.enter_dt = next_dept.enter_dt

inner join {{ source('cdw', 'visit') }} as vis on vis.visit_key = transaction_raw.visit_key
--endregion
)

select distinct
access_all.*,
room_num,
departments.dept_grp_abbr
--date(departments.enter_dt) as enter_dt,
--date(departments.exit_dt) as exit_dt

from access_all

inner join departments on departments.visit_key = access_all.visit_key
inner join {{ source('cdw', 'visit_addl_info') }} as vai on vai.visit_key = access_all.visit_key
inner join {{ source('cdw', 'master_room') }} as room on vai.last_room_key = room.room_key
where rec_dt >= date(departments.enter_dt)
and rec_dt <= date(departments.exit_dt)
and dept_grp_abbr = 'PICU'
