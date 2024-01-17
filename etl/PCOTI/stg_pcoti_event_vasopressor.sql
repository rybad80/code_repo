select
    stg_pcoti_meds_vasopressor.pat_key,
    stg_pcoti_meds_vasopressor.visit_key,
    'Vasopressor - ' || stg_pcoti_meds_vasopressor.medication_name as event_type_name,
    'VASOPRESSOR_' || upper(stg_pcoti_meds_vasopressor.medication_name) as event_type_abbrev,
    stg_pcoti_meds_vasopressor.administration_date as event_start_date,
    null as event_end_date
from
    {{ ref('stg_pcoti_meds_vasopressor') }} as stg_pcoti_meds_vasopressor
where
    stg_pcoti_meds_vasopressor.administration_date >= '2017-01-01'
