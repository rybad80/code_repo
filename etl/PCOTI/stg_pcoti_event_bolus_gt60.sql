select
    stg_pcoti_et_bolus.pat_key,
    stg_pcoti_et_bolus.visit_key,
    'Cumulative Fluid Bolus >= 60ml/kg (ICU xfer date +/- 1hr only)' as event_type_name,
    'FLUID_BOLUS_GT60' as event_type_abbrev,
    stg_pcoti_et_bolus.bolus_admin_date as event_start_date,
    null as event_end_date
from
    {{ ref('stg_pcoti_et_bolus') }} as stg_pcoti_et_bolus
