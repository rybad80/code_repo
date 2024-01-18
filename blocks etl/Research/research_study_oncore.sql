with status_study as (
    select
        protocol_id,
        min(status_date) as open_to_accrual_date
    from
        {{ source('ods', 'sv_pcl_status')}}
    where
        lower(status) = 'open to accrual'
    group by
        protocol_id
)
select
    sv_protocol.protocol_id,
    sv_protocol.protocol_no,
    sv_protocol.irb_no,
    sv_protocol.title,
    sv_protocol.short_title,
    sv_protocol.status as protocol_status,
    sv_protocol.phase,
    sv_protocol.treatment_type_desc as treatment_type_description,
    sv_protocol.investigator_initiated,
    sv_protocol.investigational_drug,
    sv_protocol.library as organizational_unit,
    status_study.open_to_accrual_date,
    rv_protocol_pi_staff.pi_name,
    sv_pcl_mgmt_mgmtgroup.mgmt_group_code,
    sv_pcl_mgmt_mgmtgroup.mgmt_group_description,
    coalesce(sv_pcl_accrual_summary.on_study_count, 0) as on_study_count,
    rv_sip_protocol.phase_desc,
    case when lower(rv_sip_protocol.phase_desc) = 'not phase based' then 'Non-Interventional'
        else 'Interventional'
    end as study_type
from
    {{ source('ods', 'sv_protocol')}} as sv_protocol
    inner join status_study
        on sv_protocol.protocol_id = status_study.protocol_id
    inner join {{ source('ods', 'rv_protocol_pi_staff')}} as rv_protocol_pi_staff
        on sv_protocol.protocol_id = rv_protocol_pi_staff.protocol_id
    inner join {{ source('ods', 'sv_pcl_mgmt_mgmtgroup')}} as sv_pcl_mgmt_mgmtgroup
        on sv_protocol.protocol_id = sv_pcl_mgmt_mgmtgroup.protocol_id
    left join {{ source('ods', 'sv_pcl_accrual_summary')}} as sv_pcl_accrual_summary
        on sv_protocol.protocol_id = sv_pcl_accrual_summary.protocol_id
    inner join {{ source ('ods', 'rv_sip_protocol')}} as rv_sip_protocol
        on sv_protocol.protocol_id = rv_sip_protocol.protocol_id
where
    lower(rv_sip_protocol.phase_desc) != 'other'
    and rv_sip_protocol.phase_desc is not null
