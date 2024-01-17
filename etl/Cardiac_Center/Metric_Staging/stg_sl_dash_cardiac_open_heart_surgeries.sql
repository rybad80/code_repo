select
    mrn,
    patient_name,
    casenum,
    surg_date,
    date(surg_date) as metric_date,
    casenum as primary_key,
    'cardiac_open_heart_surg' as metric_id
from
    {{ ref('cardiac_surgery') }}
where
    lower(open_closed) = 'open'
