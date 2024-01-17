select
    stg_cardiac_perfusion_surgery_patients.log_key,
    stg_cardiac_perfusion_surgery_patients.pat_key,
    flowsheet_vitals.weight_kg,
    flowsheet_vitals.height_cm,
    flowsheet_vitals.bsa,
    flowsheet_vitals.recorded_date
from
    {{ref('flowsheet_vitals')}} as flowsheet_vitals
    inner join {{ref('stg_cardiac_perfusion_surgery_patients')}} as stg_cardiac_perfusion_surgery_patients
        on stg_cardiac_perfusion_surgery_patients.pat_key = flowsheet_vitals.pat_key
where
    recorded_date <= perf_rec_begin_tm
