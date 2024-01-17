{{ config(meta = {
    'critical': true
}) }}

/*  worker job reference
is intended as a reference table, in preparation for an offical job group that can be good for general use
across the enterprise and isstarted from the nursing perspective but is not finalized.
It will be useful as other groups beyond nursing can use it for data exploring and for HR and Nursing
and those groups working with the provider branches of allied health, physcian, & research.
It fills in some roll-ups not available on chop_analytics..worker.
*/
select
    w.worker_id,
    w.display_name,
    w.active_ind,
    w.hire_date,
    w.termination_date,
    lvls.job_group_name,
    stg_nursing_job_code_group.nursing_job_grouper as job_group_id,
    stg_nursing_job_code_group.additional_job_group_info,
    stg_nursing_job_code_group.nursing_next_best_job_group_id,
    jprfl.rn_alt_job_group_id,
    stg_nursing_job_code_group.job_group_name_granularity_path,
    jprfl.rn_job_ind,
    jprfl.nursing_category,
    jprfl.management_level,
    jprfl.job_family_group,
    jprfl.job_family,
    jprfl.job_category_name,
    jprfl.magnet_reporting_name,
    w.job_code,
    w.job_title_display,
    jprfl.care_and_contact_type,
    jprfl.osha_category,
    jprfl.healthcare_worker_job_ind,
    cc.cost_center_display,
    cc.cost_center_type,
    cc.cost_center_group,
    cc.cost_center_parent,
    cc.full_hierarchy_level_path as all_cost_centers_hierarchy_path,
    cc.room_and_board_ind,
    cc_has_active_workers_ind,
    cc_has_active_rns_ind,
    lvls.job_group_granularity_path,
    case
    when w.active_ind = 0
    then case
        when w.termination_date > '2020-01-01'
        then 1
        else 0
        end
    end  as more_recent_term_ind,
    jprfl.nccs_direct_care_staff_ind,
    jprfl.bedside_rn_ind,
    current_date as data_as_of

from
    {{ ref('worker') }} as w
    left join {{ ref('nursing_cost_center_attributes') }} as cc
        on w.cost_center_id = cc.cost_center_id
    left join {{ ref('stg_nursing_job_code_group') }} as stg_nursing_job_code_group
        on w.job_code = stg_nursing_job_code_group.job_code
    inner join {{ ref('job_code_profile') }} as jprfl
        on w.job_code = jprfl.job_code

    /* get applicable job group hierarchy */
    left join {{ ref('job_group_levels') }} as lvls
        on stg_nursing_job_code_group.nursing_job_grouper = lvls.job_group_id
