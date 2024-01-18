with current_rollups as (
    select
        fact_department_rollup.mstr_dept_grp_unit_key,
        fact_department_rollup.dept_key,
        fact_department_rollup.rollup_nm as rollup_nm_today,
        fact_department_rollup.loc_sort_num as loc_sort_num_today,
        fact_department_rollup.unit_dept_grp_nm as unit_nm_today
    from
        {{source('cdw_analytics','fact_department_rollup')}} as fact_department_rollup
    where
        fact_department_rollup.dept_align_dt = current_date --noqa: L028
),

staffed_beds_clarity as (
    select
        dep.dept_key,
        ed_iev_event_info.event_cmt as bed_count_comment,
        dep.dept_id,
        dep.dept_nm,
        dep.dept_abbr,
        ed_iev_event_info.staffed_beds,
        ed_iev_event_info.event_id,
        ed_iev_event_info.line,
        ed_iev_event_info.event_type,
        ed_iev_event_info.event_display_name,
        ed_iev_event_info.event_time,
        cast(ed_iev_event_info.event_time as date) as event_date,
        ed_iev_event_info.event_record_time,
        ed_iev_event_info.event_user_id,
        ed_iev_pat_info.dept_event_dep_id,
        dense_rank() over (
            partition by ed_iev_pat_info.dept_event_dep_id, date(ed_iev_event_info.event_time)
            order by ed_iev_event_info.event_time desc
        ) as recrank
    from
        {{source('clarity_ods', 'ed_iev_event_info')}} as ed_iev_event_info
    left join {{source('clarity_ods', 'ed_iev_pat_info')}} as ed_iev_pat_info
        on ed_iev_event_info.event_id = ed_iev_pat_info.event_id
    left join {{source('cdw', 'department')}} as dep
        on ed_iev_pat_info.dept_event_dep_id = dep.dept_id
    where
        event_display_name = 'Staffed Beds Event'
        and ed_iev_event_info.event_time > date('2020-12-01')
),

staffed_beds_history as (
    select *,
    dense_rank() over (
            partition by lookup_licensed_bed_department_history.department_id,
            lookup_licensed_bed_department_history.eff_dt_key
            order by lookup_licensed_bed_department_history.eff_dt_key desc
    ) as recrank,
    dense_rank() over (
            partition by lookup_licensed_bed_department_history.department_id
            order by lookup_licensed_bed_department_history.eff_dt_key desc
    ) as recrank_department
    from
        {{ref('lookup_licensed_bed_department_history')}} as lookup_licensed_bed_department_history
    where lookup_licensed_bed_department_history.eff_dt_key <= to_char(current_date, 'YYYYMMDD') --noqa: L028
),

staffed_beds_all as (
    select
        staffed_beds_clarity.dept_key,
        staffed_beds_clarity.bed_count_comment as staffed_beds_comment,
        staffed_beds_clarity.dept_id as department_id,
        staffed_beds_clarity.dept_nm as department_name,
        staffed_beds_clarity.dept_abbr as department_abbreviation,
        staffed_beds_clarity.staffed_beds,
        cast(
            to_char(staffed_beds_clarity.event_date, 'YYYYMMDD')
        as bigint) as eff_dt_key,
        staffed_beds_clarity.event_date as effective_date
    from
        staffed_beds_clarity
    where
        staffed_beds_clarity.recrank = 1
    union all
    select
        department.dept_key,
        staffed_beds_history.staffed_beds_comment,
        staffed_beds_history.department_id,
        department.dept_nm as department_name,
        department.dept_abbr as department_abbreviation,
        staffed_beds_history.staffed_beds,
        staffed_beds_history.eff_dt_key,
        staffed_beds_history.effective_date
    from
        staffed_beds_history
        inner join {{source('cdw','department')}} as department
            on department.dept_id = staffed_beds_history.department_id
),

staffed_beds_lag as (
    select
        --gethist.dept_key
        staffed_beds_all.dept_key,
        staffed_beds_all.department_id,
        staffed_beds_all.eff_dt_key,
        staffed_beds_all.staffed_beds,     --, bed_cnt_comment, bed_cnt_desc
        lag(staffed_beds_all.staffed_beds, 1, null) over(
                partition by staffed_beds_all.dept_key
                order by staffed_beds_all.eff_dt_key
        ) as staffed_beds_previous,
        staffed_beds_all.department_abbreviation,
        staffed_beds_all.department_abbreviation || ' comment TBD' as default_comment
    from
        staffed_beds_all
)

select
    staffed_beds_all.department_id,
    -- beds.dept_key,
    staffed_beds_all.eff_dt_key,
    staffed_beds_all.effective_date,
    staffed_beds_all.staffed_beds,
    case
        when staffed_beds_all.staffed_beds_comment is not null
            then department.dept_abbr || ' ' || staffed_beds_all.staffed_beds_comment
        when staffed_beds_lag.staffed_beds_previous is null
            then
                coalesce(todayalign.rollup_nm_today,
                coalesce(frollup.rollup_nm,
                coalesce(department.dept_abbr,
                clarity_dep_4.int_disp_name ))) || ' opening'
        else coalesce(
            staffed_beds_all.staffed_beds_comment, staffed_beds_all.department_abbreviation || ' comment TBD')
    end as staffed_beds_comment,
    case
        when staffed_beds_lag.staffed_beds_previous is null         -- new unit
            then staffed_beds_all.staffed_beds || ' beds for ' || coalesce(
                todayalign.unit_nm_today,
                coalesce(frollup.unit_dept_grp_nm, clarity_dep_4.int_disp_name)
            )
        -- otherwise a change to current beds
        when staffed_beds_all.staffed_beds > staffed_beds_lag.staffed_beds_previous
            then 'up ' || staffed_beds_all.staffed_beds - staffed_beds_lag.staffed_beds_previous || ' bed' || case
                when abs(staffed_beds_all.staffed_beds - staffed_beds_lag.staffed_beds_previous) > 1
                    then 's' else '' end
        else 'down ' || abs(staffed_beds_all.staffed_beds - staffed_beds_lag.staffed_beds_previous) || ' bed'
            || case --noqa: L058
                when abs(staffed_beds_all.staffed_beds - staffed_beds_lag.staffed_beds_previous) > 1 then 's'
                    else ''
            end
     end as staffed_beds_change_desc
from
    staffed_beds_all
    left join staffed_beds_lag
        on staffed_beds_lag.department_id = staffed_beds_all.department_id
            and staffed_beds_lag.eff_dt_key = staffed_beds_all.eff_dt_key
    left join {{source('cdw', 'department')}} as department-- as dept
        on staffed_beds_all.dept_key = department.dept_key
    left join {{source('clarity_ods', 'clarity_dep_4')}} as clarity_dep_4-- as dep4
        on staffed_beds_all.department_id = clarity_dep_4.department_id
    left join current_rollups as todayalign
        on staffed_beds_all.dept_key = todayalign.dept_key
    left join {{source('cdw_analytics','fact_department_rollup')}} as frollup
        on staffed_beds_all.dept_key = frollup.dept_key
        and frollup.dept_align_dt = current_date - 1
