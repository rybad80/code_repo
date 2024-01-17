select
    stg_pcoti_lda_intubation.pat_key,
    stg_pcoti_lda_intubation.visit_key,
    'Intubation' as event_type_name,
    'INTUB' as event_type_abbrev,
    stg_pcoti_lda_intubation.place_dt as event_start_date,
    stg_pcoti_lda_intubation.remove_dt as event_end_date
from
    {{ ref('stg_pcoti_lda_intubation') }} as stg_pcoti_lda_intubation
where
    stg_pcoti_lda_intubation.place_dt >= '2017-01-01'
