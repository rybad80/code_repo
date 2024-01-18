/* stg_worker_latest_employment
gathers useful indicators for reporting on employment changes over the past year
this is an enterprise stage but does have numerous attributes particularly useful
to nursing.  Note that data derived from senssitive employee data fields will be in
table, worker_employment_protected
*/
select
    w.worker_id,
    j_grp.use_job_group_id as job_group_id,
    j_grp.rn_job_ind,
    j_attrb.nursing_category,
    j_attrb.nursing_category_abbreviation,
    w.termination_date,
    w.active_ind,
    w.hire_date,
    w.scheduled_weekly_hours,
    case when w.scheduled_weekly_hours >= 36
        then 1 else 0 end as nursing_full_time_ind,
    case
        when w.active_ind = 0 and w.termination_date >= current_date - 30
        then 1 end as term_last_thirty_days_ind,
    case
        when w.active_ind = 0 and w.termination_date >= add_months(current_date, -12)
        then 1 end as term_in_last_year_ind,

    case when w.active_ind = 0
        then case
            when w.termination_date >= add_months(current_date, -12)
            then 'term after ' || to_char(add_months(current_date, -12) - 1, 'MM/DD/YY')
            else 'term before ' || to_char(add_months(current_date, -12), 'MM/DD/YY') end
        end as term_in_last_year_label,
    case
        when w.hire_date >= add_months(current_date, -12)
        then 1
        end as hire_in_last_year_ind,

    /* recent equals in the last twelve completed months or in the current month */
    case
        when term_in_last_year_ind = 1
            or w.termination_date >= add_months(date_trunc('month', current_date), -12)
        then date_trunc('month', w.termination_date) end as recent_termination_month,
    case
        when hire_in_last_year_ind = 1
            or w.hire_date >= add_months(date_trunc('month', current_date), -12)
        then date_trunc('month', w.hire_date) end as recent_hire_month,

    case
        when w.hire_date >= add_months(current_date, -12)
        then 'hired in past year'
        else 'hired before ' || to_char(add_months(current_date, -12), 'MM/DD/YY')
        end as hire_label,
    case
        when w.hire_date >= add_months(current_date, -12) then 'new hire (< 1 yr)'
        else 'over a year at CHOP'
        end as hire_timeframe_label,

    case
        when w.hire_date > w.original_hire_date
        then 1
        end as employment_rehired_ind,

    case
        when term_in_last_year_ind = 1 or hire_in_last_year_ind = 1
        then 1 end as employment_change_last_year_ind,
    case w.rn_job_ind
        when 1 then 'RN'
        else j_attrb.nursing_category_abbreviation
        end as nursing_role_type,
    case w.nccs_direct_care_staff_ind
         when 1 then 'direct patient care'
         else 'not direct care'
        end as nursing_direct_care_type,
    case w.active_ind
         when 1 then 'here' else 'no longer here'
         end as active_label,
    case
        when term_in_last_year_ind = 1
        then case
            when hire_in_last_year_ind = 1
            then 0
            else -1 /* only a subtract if here over a year, otherwise term cancels out the hire */
            end
        when hire_in_last_year_ind = 1
        then 1 /* added to the total */
        end as employment_net_in_past_year_count,

    mgr_w.worker_id as direct_supervisor_worker_id,
    mgr_w.display_name_formatted as direct_supervisor_name_formatted,
    w.worker_wid,
    w.position_wid,
    w.total_years_as_employee,

    case
        when w.total_years_as_employee < 1 then 'Less than One Year'
        when w.total_years_as_employee between 1 and 3 then 'One to Three Years'
        when w.total_years_as_employee between 3.01 and 5 then '> Three to Five Years'
        when w.total_years_as_employee between 5.01 and 10 then '> Five to Ten Years'
        when w.total_years_as_employee > 10 then 'Over Ten Years'
        else w.total_years_as_employee || 'years' end as tenure_category,
    case
        when w.total_years_as_employee  < 1 then 9
        when w.total_years_as_employee between 1 and 3 then 12
        when w.total_years_as_employee between 3.01 and 5 then 15
        when w.total_years_as_employee between 5.01 and 10 then 18
        when w.total_years_as_employee > 10 then 21
        else 6 end as tenure_sort_num,

    w.employee_ind,
     case j_grp.nursing_category
        when 'Supervisor and Manager' then 1 else 0 end as cln_direct_mgmt_ind,
    case
        when w.management_level = 'Supv / Team Lead' and cln_direct_mgmt_ind = 1
        then 1 else 0 end as nurse_supervisor_ind,
    case
        when w.management_level = 'Mid-Management' and cln_direct_mgmt_ind = 1
        then 1 else 0 end as nurse_manager_ind

from
    {{ ref('worker') }} as w
    left join {{ ref('worker') }} as mgr_w
        on w.manager_worker_wid = mgr_w.worker_wid
    left join {{ ref('job_code_profile') }}  as j_attrb
        on w.job_code = j_attrb.job_code
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as j_grp
        on w.job_code = j_grp.job_code
