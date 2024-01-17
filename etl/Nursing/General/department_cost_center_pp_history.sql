/* department_cost_center_pp_history
based on earliest staff Kronos time or earliest/latest encounter date
contains history rows by pay period for associating
Epic department to CHOP cost center and the department group to apply for the period
*/
select
    dept_cc_hist.epic_department_specialty_name,
    dept_cc_hist.department_abbr,
    dept_cc_hist.department_name,
    -- begin code identical to current logic of stg_department_staffing
    case
        when dept_cc_hist.historical_intended_use_name in (
            'Primary Care',
            'Urgent Care',
            'Emergency')
        then dept_cc_hist.historical_intended_use_name
        when dept_cc_hist.historical_intended_use_name
            = 'Outpatient Specialty Care'
        then dept_cc_hist.historical_care_area_name /* full care area name for speciality */
        else dept_cc_hist.historical_care_area_abbr /* else use VCC abbreviation */
    end as department_group,
    coalesce(get_use_grouper.set_dept_use_grouper, 'unknown') as department_use_grouper,
    case dept_cc_hist.historical_care_area_name
        when 'NA' then department_use_grouper
        when 'Unknown' then department_use_grouper
        else case
            when dept_cc_hist.historical_intended_use_name = 'Overflow' then 'IP Overflow'
            else department_group
        end
    end as care_area_or_use,
    coalesce(get_use_grouper.hospital_unit_ind, 0) as hospital_unit_ind,
    case department_use_grouper
        when 'OP Specialty Care' then 1 else 0
    end as specialty_care_ind,
    -- end code identical to current logic of stg_department_staffing

    dept_cc_hist.pp_end_dt_key,
    dept_cc_hist.cost_center_display,
    dept_cc_hist.cost_center_type,
    dept_cc_hist.cost_center_group,
    dept_cc_hist.pp_end_dt,
    dept_cc_hist.historical_care_area_abbr,
    dept_cc_hist.historical_care_area_name,
    dept_cc_hist.historical_intended_use_name,
    dept.department_id,
    dept_cc_hist.cost_center_id,
    dept_cc_hist.cost_center_site_id,
    dept_cc_hist.clarity_cost_center_id
from
    {{ ref('stg_department_staffing') }} as dept
    inner join {{ ref('stg_department_cost_center_history') }} as dept_cc_hist
        on dept.department_id = dept_cc_hist.department_id
    left join {{ ref('lookup_department_use_grouper') }} as get_use_grouper
        on dept_cc_hist.historical_intended_use_name = get_use_grouper.intended_use_name
    left join {{ ref('stg_nursing_dept_cc_p2_ends') }} as d_cc_when_end
        on dept.department_id = d_cc_when_end.department_id
    inner join {{ ref('stg_nursing_dept_cc_p3_add_start') }} as d_cc_when
        on dept.department_id = d_cc_when.department_id
        and dept_cc_hist.pp_end_dt_key
            between d_cc_when.history_start_dt_key
            and d_cc_when.take_to_end_dt_key
where
    coalesce(d_cc_when_end.build_dept_cc_history_ind, 1) = 1
