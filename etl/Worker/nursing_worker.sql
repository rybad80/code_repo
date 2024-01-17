/* nursing_worker
gethers the attributes needed to support the main RN profile and employment data in the Nursing Dashboard
and filtering by various managment levels
Note: for employee protested data, use worker_employement_protected only when needed for an application
*/
with
supvsr_info as (
    select
        mgr.mgr_key as get_mgr_key,
        mgr.emp_key as mgr_emp_key,
        mgr_w.worker_id as get_mgr_worker_id,
        mgr_w.management_level as mgr_management_level,
        case
            when mgr_empl_data.nurse_supervisor_ind = 1
            then 1 else 0 end as mgr_is_nurse_supervisor_ind,
        case
            when mgr_empl_data.nurse_manager_ind = 1
            then 1 else 0 end as mgr_is_nurse_manager_ind,
        case
            when mgr_w.management_level in ('AVP / SrDir / Dir', 'VP', 'SVP')
                and mgr_w.nursing_category = 'Director and Executive'
            then 1 else 0 end as mgr_is_nurse_top_mgmt_ind
    from
        {{ source('cdw', 'manager') }} as mgr
        inner join {{ ref('worker') }} as mgr_w
            on mgr.emp_key = mgr_w.workday_emp_key
        left join {{ ref('stg_worker_latest_employment') }}	as mgr_empl_data
            on mgr_w.worker_wid = mgr_empl_data.worker_wid
)

select

    w.worker_id,
    w.workday_emp_key,
    w.clarity_emp_key,
    w.worker_id as emp_id, /* will drop one removed in N Dashboard */
    w.ad_login,
    w.legal_reporting_name,

    w.total_years_as_employee as years_at_chop,
    empl_data.tenure_category,
    empl_data.tenure_sort_num,

    empl_data.term_last_thirty_days_ind,
    empl_data.term_in_last_year_ind,
    empl_data.term_in_last_year_label,
    empl_data.active_label,

    empl_data.hire_in_last_year_ind,
    empl_data.hire_label,
    empl_data.hire_timeframe_label,
    empl_data.employment_rehired_ind,
    empl_data.employment_change_last_year_ind,

    empl_data.employment_net_in_past_year_count,

    empl_data.job_group_id,
    empl_data.rn_job_ind,
    empl_data.nursing_category_abbreviation,

    w.job_code,
    w.job_title_display,
    w.job_family,
    w.job_exempt_ind,
    w.cost_center_id,
    w.cost_center_site_id,
    w.fte_percentage as full_time_equivalent_percentage,
    w.scheduled_weekly_hours,
    empl_data.nursing_full_time_ind,
    empl_data.nursing_role_type,
    empl_data.nursing_direct_care_type,
    coalesce(j_grp_rollup.staff_nurse_ind, 0) as bedside_nurse_ind,

    w.termination_date,
    w.active_ind,
    w.hire_date,

    empl_data.recent_hire_month,
    empl_data.recent_termination_month,
    empl_data.direct_supervisor_worker_id,
    empl_data.direct_supervisor_name_formatted,
    w.reporting_chain,
    w.worker_wid,
    w.position_title,
    w.employee_ind,
    w.worker_type, /* WORKER_TYPE_CATEGORY 'Employee' or 'Contingent Worker', */
    w.worker_role, /* WORKER_TYPE under Employee or CW */

    empl_data.cln_direct_mgmt_ind,
    case empl_data.nursing_category
        when 'Director and Executive' then 1 else 0 end as cln_upper_mgmt_ind,
    case empl_data.job_group_id
        when 'CNE' then 1 else 0 end as cln_expert_ind,
    case empl_data.job_group_id
        when 'CNS' then 1 else 0 end as cln_specialist_ind,
    case empl_data.job_group_id
        when 'nurseMidwife' then 1
        when 'NP' then 1 else 0 end as cln_practitioner_midwife_ind,
    case empl_data.job_group_id
        when 'ENS' then 1 else 0 end as cln_unit_educator_ind,
    case empl_data.job_group_id
        when 'NPDS' then 1 else 0 end as cln_system_educator_ind,
    case empl_data.job_group_id
        when 'SQS' then 1 else 0 end as safety_quality_specialist_ind,
    case
        when --empl_data.rn_job_ind = 0 and w.nccs_direct_care_staff_ind = 1
        j_grp_rollup.nursing_job_rollup = 'UAP'
        then 1 else 0 end as uap_ind,
    w.is_manager_ind,
    empl_data.nurse_supervisor_ind,
    empl_data.nurse_manager_ind,

    supvsr_info.mgr_management_level,
    coalesce(supvsr_info.mgr_is_nurse_supervisor_ind, 0) as mgr_is_nurse_supervisor_ind,
    coalesce(supvsr_info.mgr_is_nurse_manager_ind, 0) as mgr_is_nurse_manager_ind,
    case
        when supvsr_info.mgr_is_nurse_supervisor_ind = 1 then 1
        when supvsr_info.mgr_is_nurse_manager_ind = 1 then 1
        else 0 end as mgr_is_nurse_supervisor_or_manager_ind,
    coalesce(supvsr_info.mgr_is_nurse_top_mgmt_ind, 0) as mgr_is_nurse_top_mgmt_ind,
    case when w.magnet_reporting_ind = 1 and empl_data.nursing_category is not null
        then 1 else 0 end as nursing_dashboard_use_ind

from
    {{ ref('worker') }} as w
    left join {{ ref('stg_worker_latest_employment') }} as empl_data
        on w.worker_wid = empl_data.worker_wid

    left join supvsr_info
        on w.mgr_key = supvsr_info.get_mgr_key

    left join {{ ref('job_group_levels_nursing') }} as j_grp_rollup
        on empl_data.job_group_id = j_grp_rollup.job_group_id
