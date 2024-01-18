{{ config(meta = {
    'critical': true
}) }}

with all_history as (
    select
        reg_data_hx_membership.record_id,
        reg_data_hx_membership.contact_date_real,
        reg_data_hx_membership.line,
        reg_data_hx_membership.registry_id,
        registry_data_info.networked_id,
        reg_data_hx_membership.contact_date,
        registry_data_info.networked_csn as csn,
        registry_data_info.networked_id as pat_id, -- currently all related_ini = 'EPT'
        reg_data_hx_membership.change_instant_utc_dttm as enrollment_start,
        lead(reg_data_hx_membership.change_instant_utc_dttm) over (
            partition by registry_data_info.networked_id, reg_data_hx_membership.registry_id
            order by reg_data_hx_membership.change_instant_utc_dttm
        ) as enrollment_end,
        reg_data_hx_membership.status_c as status_id,
        case reg_data_hx_membership.status_c when 1 then 'Active' when 2 then 'Inactive' end as status_name,
        lead(case reg_data_hx_membership.status_c when 1 then 'Active' when 2 then 'Inactive' end) over (
            partition by registry_data_info.networked_id, reg_data_hx_membership.registry_id
            order by reg_data_hx_membership.change_instant_utc_dttm
        ) as next_status,
        lead(reg_data_hx_membership.status_reason_c) over (
            partition by registry_data_info.networked_id, reg_data_hx_membership.registry_id
            order by reg_data_hx_membership.change_instant_utc_dttm
        ) as change_reason
    from
        {{ source('clarity_ods', 'reg_data_hx_membership') }} as reg_data_hx_membership
        inner join {{ source('clarity_ods', 'registry_data_info') }} as registry_data_info using(record_id)
    where
        {{ limit_dates_for_dev(ref_date = 'reg_data_hx_membership.change_instant_utc_dttm') }}
)

select
    {{
        dbt_utils.surrogate_key([
            'all_history.record_id',
            'all_history.contact_date_real',
            'all_history.line'
        ])
    }} as epic_registry_hx_key,
    -- about patient
    stg_patient_ods.mrn,
    stg_patient_ods.patient_name,
    stg_patient_ods.dob,
    (all_history.enrollment_start::date - stg_patient_ods.dob::date) / 365.25 as age_years_at_enrollment,
    -- about registry
    registry_config.registry_id as epic_registry_id,
    registry_config.registry_name,
    all_history.enrollment_start,
    all_history.enrollment_end,
    row_number() over (
            partition by all_history.pat_id, registry_config.registry_id
            order by all_history.enrollment_start
        ) as staus_seq_number,
    case
        when row_number() over (
                partition by all_history.pat_id, registry_config.registry_id
                order by all_history.enrollment_start desc
            ) = 1 then 1
        else 0
    end as current_record_ind,
    -- granularity
    all_history.record_id,
    all_history.contact_date_real,
    all_history.line
from
    all_history
    inner join {{ ref('stg_patient_ods') }} as stg_patient_ods using (pat_id)
    inner join {{ source('clarity_ods', 'registry_config') }} as registry_config using (registry_id)
where
    -- only needs active events (start dates), lead() in CTE has captured the inactive events / end dates
    all_history.status_id = 1
