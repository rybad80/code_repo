select
    neo_nicu_episode.pat_key,
    lookup_neo_nicu_or_procedures.procedure_name as cohort_group_name,
    lookup_neo_nicu_or_procedures.procedure_display_name as cohort_group_display_name,
    min(surgery_procedure.surgery_date) as cohort_group_enter_date
from
    {{ ref('neo_nicu_episode') }} as neo_nicu_episode
    inner join {{ ref('surgery_procedure') }} as surgery_procedure
        on surgery_procedure.visit_key = neo_nicu_episode.visit_key
    inner join {{ ref('lookup_neo_nicu_or_procedures') }} as lookup_neo_nicu_or_procedures
        on lookup_neo_nicu_or_procedures.or_proc_id = surgery_procedure.or_proc_id
where
    lower(surgery_procedure.case_status) = 'completed'
group by
    neo_nicu_episode.pat_key,
    lookup_neo_nicu_or_procedures.procedure_name,
    lookup_neo_nicu_or_procedures.procedure_display_name
