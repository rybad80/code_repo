select
    'Unique Patients with EEG' as metric_name,
    case when drill_down = 'Routine' then 'Routine EEG Patients'
        when drill_down = 'Ambulatory' then 'Ambulatory EEG Patients'
        when drill_down = 'Inpatient' then 'Inpatient cEEG Patients'
        end as drill_down,
    pat_key,
    service_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as desired_direction,
    pat_key as primary_key
from
    {{ ref('stg_neuro_cont_eeg')}}
group by
  pat_key,
  service_date,
  drill_down
