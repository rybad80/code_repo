select distinct
    'New Interventional Clinical Trials (Oncore Registered)' as trial_metric_name,
    'New Interventional Clinical Trial Participants (Oncore Registered)' as participants_metric_name,
    protocol_id as primary_key,
    irb_no,
    title,
    open_to_accrual_date as metric_date,
    pi_name,
    mgmt_group_description,
    mgmt_group_code,
    on_study_count,
    phase_desc,
    study_type,
    'cardiac_interv_studies' as trial_metric_id,
    'cardiac_interv_participants' as participants_metric_id

from
    {{ref('research_study_oncore')}}
where
    mgmt_group_code in (
        '105-000', -- Cardiac Critical Care Medicine
        '403-000', -- Cardiology
        '701-000', -- Cardiothoracic Surgery
        '103-000' -- CT Anesthesiology
        )
    and lower(study_type) = 'interventional'
