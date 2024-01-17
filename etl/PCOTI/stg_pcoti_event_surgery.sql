select
    surgery_encounter_timestamps.pat_key,
    surgery_encounter_timestamps.visit_key,
    'Surgery - Procedure' as event_type_name,
    'SURG_PROC' as event_type_abbrev,
    surgery_encounter_timestamps.procedure_start_date as event_start_date,
    surgery_encounter_timestamps.procedure_close_date as event_end_date
from
    {{ ref('surgery_encounter_timestamps') }} as surgery_encounter_timestamps
where
    surgery_encounter_timestamps.surgery_date >= '2017-01-01'
    and surgery_encounter_timestamps.procedure_start_date is not null

union all

select
    surgery_encounter_timestamps.pat_key,
    surgery_encounter_timestamps.visit_key,
    'Surgery - Anesthesia' as event_type_name,
    'SURG_ANES' as event_type_abbrev,
    surgery_encounter_timestamps.anesthesia_start_date as event_start_date,
    surgery_encounter_timestamps.anesthesia_stop_date as event_end_date
from
    {{ ref('surgery_encounter_timestamps') }} as surgery_encounter_timestamps
where
    surgery_encounter_timestamps.surgery_date >= '2017-01-01'
    and surgery_encounter_timestamps.anesthesia_start_date is not null
