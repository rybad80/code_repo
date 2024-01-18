select distinct
    'New Interventional Clinical Trials (Oncore Registered)' as trials_metric_name,
    'New Interventional Clinical Trial Participants (Oncore Registered)' as participants_metric_name,
    protocol_no,
    on_study_count,
    open_to_accrual_date,
    protocol_id as primary_key,
    'neuro_interv_studies' as trials_metric_id,
    'neuro_interv_participants' as participants_metric_id
from
    {{ ref('research_study_oncore')}}
where
    lower(organizational_unit) = 'research institute'
    and mgmt_group_code in ('415-000', '705-000') --Neurology, Neurosurgery
    and lower(study_type) = 'interventional'
