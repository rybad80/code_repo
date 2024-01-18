{#
this data currently comes from clarity only. The legacy medication tables also have data from
SCM  (sunrise clinical manager). If we decide to integrate the SCM data, this logic should be put into a stage table
and a similar table should be added for SCM and then exposed through this fact table with a union
#}

select
    -- ??? add a source lookup with source names / ids / description
    'clarity' as source_name,
    {{
        dbt_utils.surrogate_key([
            'source_name',
            'mar_admin_info.order_med_id',
            'mar_admin_info.line'
        ])
    }} as medication_administration_key,
    mar_admin_info.order_med_id::varchar(100) as medication_order_id,
    coalesce(mar_admin_info.line, 0) as admin_seq_number,
    source_name
        || coalesce(mar_admin_info.order_med_id::varchar(100) || '-', '')
        || coalesce(mar_admin_info.line, 0)
        as integration_id,
    --admin
    case
        when coalesce(mar_custom_actions.ip_mar_mapping_c, mar_admin_info.mar_action_c) in (
            1, -- given
            6, -- new bag
            12, -- bolus
            13  -- push
        ) then 1
        else 0
        end as given_ind,
    case
        when
            min(mar_admin_info.line) over(
                partition by mar_admin_info.order_med_id, given_ind
            ) = mar_admin_info.line
            and given_ind = 1
        then 1
        else 0
        end as first_given_ind, -- ??? double check
    zc_mar_rslt.name as admin_result,
    zc_mar_rslt.result_c as admin_result_id,
    mar_admin_info.taken_time as admin_date,
    mar_admin_info.sig as admin_dose,
    zc_med_unit.name as admin_dose_unit,
    mar_admin_info.infusion_rate as admin_infusion_rate,
    zc_admin_route.name as admin_route,
    admin_route_groupers.route_group as admin_route_group,
    clarity_dep.department_name as admin_department,
    mar_admin_info.mar_admin_dept_id as admin_department_id,
    {{ dbt_utils.surrogate_key([
        'source_name', 'mar_admin_info.order_med_id'
    ]) }} as medication_order_key
from
    {{source('clarity_ods', 'mar_admin_info')}} as mar_admin_info  -- scheduled admins, may include skipped, dropped --noqa: L016
    left join {{source('clarity_ods', 'zc_med_unit')}} as zc_med_unit
        on zc_med_unit.disp_qtyunit_c = mar_admin_info.dose_unit_c
    left join {{source('clarity_ods', 'zc_admin_route')}} as zc_admin_route
        on zc_admin_route.med_route_c = mar_admin_info.route_c
    left join {{ source('clarity_ods', 'clarity_dep') }} as clarity_dep
        on mar_admin_info.mar_admin_dept_id = clarity_dep.department_id
    left join {{ source('clarity_ods', 'zc_mar_rslt') }} as zc_mar_rslt
        on zc_mar_rslt.result_c = mar_admin_info.mar_action_c
    left join {{ source('clarity_ods', 'mar_custom_actions') }} as mar_custom_actions
        on mar_custom_actions.ip_mar_actions_c = mar_admin_info.mar_action_c
    left join {{ ref('lookup_medication_route_groupers') }} as admin_route_groupers
        on admin_route_groupers.source_id = mar_admin_info.route_c
        and admin_route_groupers.source_system = 'CLARITY'
where
    {{ limit_dates_for_dev(ref_date = 'mar_admin_info.taken_time') }}
