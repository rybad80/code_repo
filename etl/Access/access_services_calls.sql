select
    access_intake_cont_call_type_period.access_intake_cont_call_type_key,
    access_intake_cont_call_type_period.period_start_dt,
    hour(
        access_intake_cont_call_type_period.period_start_dt
    ) as period_start_hr,
    date_trunc(
        'month', access_intake_cont_call_type_period.period_start_dt
    ) as calendar_month,
    access_intake_cont_call_type.contact_call_type_id as call_type_id,
    access_intake_cont_call_type.enterprise_nm as call_type,
    access_intake_cont_call_type.enterprise_desc as call_type_desc,
    dim_call_center_group.call_cntr_grp_id as call_center_group_id,
    dim_call_center_group.enterprise_nm as call_center_group,
    alt_nm_rollup.call_cntr_cat as call_center_group_alt_nm,
    dim_call_center_group.enterprise_desc as call_center_group_desc,
    org_rollup.call_cntr_cat as org_grouper,
    ccgroup_rollup.call_cntr_cat as call_center_grouper,
    dept_rollup.call_cntr_cat as dept_grouper,
    access_intake_cont_call_type_period.calls_offered_cnt,
    access_intake_cont_call_type_period.calls_handled_cnt,
    access_intake_cont_call_type_period.calls_agent_answered_cnt,
    access_intake_cont_call_type_period.total_calls_abandoned_cnt,
    access_intake_cont_call_type_period.max_calls_queued_cnt,
    access_intake_cont_call_type_period.service_level_calls_answered_cnt,
    access_intake_cont_call_type_period.service_level_calls_offered_cnt,
    access_intake_cont_call_type_period.service_level_abandoned_cnt,
    (
        access_intake_cont_call_type_period.service_level_calls_offered_cnt
        - access_intake_cont_call_type_period.service_level_abandoned_cnt
    ) as service_level_calls_could_answer,
    access_intake_cont_call_type_period.call_handled_seconds,
    access_intake_cont_call_type_period.answer_wait_seconds,
    access_intake_cont_call_type_period.max_call_wait_seconds,
    access_intake_cont_call_type_period.abandoned_call_delay_seconds,
    /* Call Center's standard hours are 8 AM to 7 PM */
    case
        when
            hour(
                access_intake_cont_call_type_period.period_start_dt
            ) between 8 and 18  then 1
        else 0
    end as standard_hours_ind,
    case
        when
            lower(
                master_date.day_nm
            ) in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday') then 1
        else 0
    end as weekday_ind,
    case
        when standard_hours_ind = 1 and weekday_ind = 1  then 1 else 0
    end as standard_call_center_day_ind,
    case
        when
            master_call_type_scheduling.access_intake_cont_call_type_key is not null then 1
        /*
            as of June 2018, all the Call Types for Access Center are scheduling
            assumed for Primary Care also
        */
        when lower(ccgroup_rollup.call_cntr_cat) = 'access center' then 1
        when lower(ccgroup_rollup.call_cntr_cat) = 'primary care' then 1
        when access_intake_cont_call_type.dim_call_cntr_grp_key = 0 then 1
        when access_intake_cont_call_type_period.access_intake_cont_call_type_key = 0 then 1
        else 0
    end as scheduling_ind,
    access_intake_cont_call_type_period.dim_call_type_call_cntr_grp_key,
    access_intake_cont_call_type.dim_call_cntr_grp_key,
    access_intake_cont_call_type_period.dim_access_intake_service_level_type_key
from
    {{source('cdw', 'access_intake_cont_call_type_period')}} as access_intake_cont_call_type_period
    inner join {{source('cdw', 'access_intake_cont_call_type')}} as access_intake_cont_call_type
        on access_intake_cont_call_type.access_intake_cont_call_type_key
            = access_intake_cont_call_type_period.access_intake_cont_call_type_key
    left join {{source('cdw', 'dim_call_center_group')}} as dim_call_center_group
        on dim_call_center_group.dim_call_cntr_grp_key
            = access_intake_cont_call_type_period.dim_call_type_call_cntr_grp_key
        and dim_call_center_group.dim_call_cntr_grp_key > 0
    left join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt = access_intake_cont_call_type_period.period_start_dt
    left join {{source('cdw_analytics', 'master_call_type_scheduling')}} as master_call_type_scheduling
        on master_call_type_scheduling.access_intake_cont_call_type_key
            = access_intake_cont_call_type_period.access_intake_cont_call_type_key
    left join {{source('cdw_analytics', 'master_call_center_group_category')}} as ccgroup_rollup
        on ccgroup_rollup.dim_call_cntr_grp_key = dim_call_center_group.dim_call_cntr_grp_key
        and lower(ccgroup_rollup.call_cntr_cat_type) = 'ccgrprollup'
    left join {{source('cdw_analytics', 'master_call_center_group_category')}} as org_rollup
        on org_rollup.dim_call_cntr_grp_key = dim_call_center_group.dim_call_cntr_grp_key
        and lower(org_rollup.call_cntr_cat_type) = 'orgrollup'
    left join {{source('cdw_analytics', 'master_call_center_group_category')}} as dept_rollup
        on dept_rollup.dim_call_cntr_grp_key = dim_call_center_group.dim_call_cntr_grp_key
        and lower(dept_rollup.call_cntr_cat_type) = 'deptrollup'
    left join {{source('cdw_analytics', 'master_call_center_group_category')}} as alt_nm_rollup
        on alt_nm_rollup.dim_call_cntr_grp_key = dim_call_center_group.dim_call_cntr_grp_key
        and lower(alt_nm_rollup.call_cntr_cat_type) = 'altnm'
