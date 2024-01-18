{{ config(meta = {
    'critical': false
}) }}

select distinct
    'New Interventional Clinical Trials (Oncore Registered)' as trial_metric_name,
    'New Interventional Clinical Trial Participants (Oncore Registered)' as participants_metric_name,
    protocol_no,
    on_study_count,
    open_to_accrual_date,
    protocol_id as primary_key,
    'onco_interv_studies' as trial_metric_id,
    'onco_interv_participants' as participants_metric_id
from
    {{ ref('research_study_oncore')}}
where
    lower(organizational_unit) = 'oncology'
    and lower(study_type) = 'interventional'
