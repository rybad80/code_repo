select
    'EEG Volume' as metric_name,
    case when drill_down = 'Routine' then 'Routine EEG Visits'
        when drill_down = 'Ambulatory' then 'Ambulatory EEG Days'
        when drill_down = 'Inpatient' then 'Inpatient cEEG Days'
        end as drill_down,
    count_eeg_days_id,
    service_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as desired_direction,
    count_eeg_days_id as primary_key
from
    {{ ref('stg_neuro_cont_eeg')}}
group by
  count_eeg_days_id,
  service_date,
  drill_down
