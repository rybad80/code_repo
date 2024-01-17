select distinct
    'New Non-Interventional Research Studies (Oncore Registered)' as studies_metric_name,
    'New Non-Interventional Research Study Participants (Oncore Registered)' as participants_metric_name,
    protocol_no,
    on_study_count,
    open_to_accrual_date,
    protocol_id as primary_key,
    'neuro_non_interv_studies' as studies_metric_id,
    'neuro_non_interv_participants' as participants_metric_id
from
    {{ ref('research_study_oncore')}}
where
    lower(organizational_unit) = 'research institute'
    and mgmt_group_code in ('415-000', '705-000') --Neurology, Neurosurgery
    and lower(study_type) = 'non-interventional'
