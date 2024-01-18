select
    encounter_ed.visit_key,
    encounter_ed.pat_key,
    'ED_SEEN' as cohort,
    null as subcohort
from
    {{ ref('encounter_ed') }} as encounter_ed
