{#
this data currently comes from clarity only. The legacy medication tables also have data from
SCM  (sunrise clinical manager). If we decide to integrate the SCM data, this logic should be put into a stage table
and a similar table should be added for SCM and then exposed through this fact table with a union
#}


select
    'clarity' as source_name,
    {{
        dbt_utils.surrogate_key([
            'source_name',
            'order_med.order_med_id'
        ])
    }} as medication_order_key,
    order_med.order_med_id::varchar(150) as medication_order_id,
    source_name || '-' || coalesce(order_med.order_med_id::varchar(150), '') as integration_id,
    order_med.pat_enc_csn_id as csn,
    order_med.order_inst as medication_order_create_date,
    order_med.description as medication_order_name,
    order_med.order_start_time as medication_start_date,
    order_med.order_end_time as medication_end_date,
    zc_order_status.name as order_status,
    order_med.hv_discrete_dose as order_dose,
    zc_med_unit.name as order_dose_unit,
    zc_admin_route.name as order_route,
    lookup_medication_route_groupers.route_group as order_route_group,
    case when order_med.non_formulary_yn = 'N' then 1 else 0 end as formulary_med_ind,
    order_med.quantity,
    order_med.refills as n_refills_allowed,
    order_med.refills_remaining as n_refills_remaining,
    order_med.pharmacy_id,
    rx_phr.pharmacy_name,
    ip_frequency.freq_name as order_frequency,
    zc_ordering_mode.ordering_mode_c as order_mode_id,
    zc_ordering_mode.name as order_mode,
    zc_order_class.name as order_class,
    case when zc_order_class.order_class_c in (3, 23) then 1 else 0 end as historical_med_ind,
    case when order_med_3.ctrl_med_yn = 'Y' then 1 else 0 end as control_med_ind,
    zc_e_pres_dea_code.e_pres_dea_code_c as dea_code_id,
    zc_e_pres_dea_code.name as dea_class_code,
    zc_active_order.name as active_order_status,
    case when order_med.is_pending_ord_yn = 'Y' then 1 else 0 end as pending_ind,
    order_med.sched_start_tm as scheduled_start_time,
    timezone(order_med.discon_time, 'UTC', 'America/New_York') as discontinue_date,
    order_metrics.prl_orderset_id as orderset_id,
    cl_prl_ss.protocol_name as orderset_name,
    order_med.ord_prov_id as ordering_provider_id,
    ord_prov.prov_name as ordering_provider_name,
    order_med.authrzing_prov_id as authorizing_provider_id,
    auth_prov.prov_name as authorizing_provider_name,
    order_med.med_comments as order_comments,
    clarity_dep.department_name as patient_department,
    order_med.pat_loc_id as patient_department_id,
    case when order_med.weight_based_yn = 'Y' then 1 else 0 end as weight_based_ind,
    {{ dbt_utils.surrogate_key(['source_name', 'order_medinfo.dispensable_med_id']) }} as medication_key
from
    {{ source('clarity_ods', 'order_med') }} as order_med
    left join  {{ source('clarity_ods', 'order_med_3') }} as order_med_3
        on order_med_3.order_id = order_med.order_med_id
    left join  {{ source('clarity_ods', 'order_med_4') }} as order_med_4
        on order_med_4.order_id = order_med.order_med_id
    left join {{ source('clarity_ods', 'order_medinfo') }} as order_medinfo
        on order_medinfo.order_med_id = order_med.order_med_id
    left join {{ source('clarity_ods', 'clarity_dep') }} as clarity_dep
        on order_med.pat_loc_id = clarity_dep.department_id
    --other demographics
    left join {{ source('clarity_ods', 'order_metrics') }} as order_metrics
        on order_metrics.order_id = order_med.order_med_id
    left join {{source('clarity_ods', 'cl_prl_ss')}} as cl_prl_ss
        on cl_prl_ss.protocol_id = order_metrics.prl_orderset_id
    left join {{source('clarity_ods', 'ip_frequency')}} as ip_frequency
        on ip_frequency.freq_id = order_med.hv_discr_freq_id
    left join {{source('clarity_ods', 'clarity_ser')}} as auth_prov
        on auth_prov.prov_id = order_med.authrzing_prov_id
    left join {{source('clarity_ods', 'clarity_ser')}} as ord_prov
        on ord_prov.prov_id = order_med.ord_prov_id
    left join {{source('clarity_ods', 'rx_phr')}} as rx_phr
        on rx_phr.pharmacy_id = order_med.pharmacy_id
    --dictionaries (not always filled in)
    left join {{source('clarity_ods', 'zc_med_unit')}} as zc_med_unit
        on zc_med_unit.disp_qtyunit_c = order_med.hv_dose_unit_c
    left join {{source('clarity_ods', 'zc_admin_route')}} as zc_admin_route
        on zc_admin_route.med_route_c = order_med.med_route_c
    left join {{source('clarity_ods', 'zc_ordering_mode')}} as zc_ordering_mode
        on zc_ordering_mode.ordering_mode_c = order_med.ordering_mode_c
    left join {{source('clarity_ods', 'zc_order_status')}} as zc_order_status
        on zc_order_status.order_status_c = order_med.order_status_c
    left join {{source('clarity_ods', 'zc_active_order')}} as zc_active_order
        on zc_active_order.active_order_c = order_med.act_order_c
    left join {{source('clarity_ods', 'zc_order_class')}} as zc_order_class
        on zc_order_class.order_class_c = order_med.order_class_c
    left join {{source('clarity_ods', 'zc_e_pres_dea_code')}} as zc_e_pres_dea_code
        on zc_e_pres_dea_code.e_pres_dea_code_c = order_med_4.e_pres_dea_code_c
    left join {{ref('lookup_medication_route_groupers')}} as lookup_medication_route_groupers
        on lookup_medication_route_groupers.source_id = order_med.med_route_c
        and lookup_medication_route_groupers.source_system = 'CLARITY'
where
    {{ limit_dates_for_dev(ref_date = 'order_med.order_inst') }}
