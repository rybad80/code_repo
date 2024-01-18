with surgery_diagnoses as(
    select
        surgery_procedure.or_key,
        max(
            case when lower(lookup_neuro_dx_grouping.dx_grouping) = 'brain tumor'
                and diagnosis_encounter_all.marked_primary_ind = 1 then 1 else 0 end
        ) as brain_tumor_enc,
        max(
            case when lower(lookup_neuro_dx_grouping.subgrouping) like '%epilepsy%' then 1 else 0 end
        ) as epilepsy_enc
    from
        {{ ref('surgery_procedure')}} as surgery_procedure
        inner join {{ ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on surgery_procedure.visit_key = diagnosis_encounter_all.visit_key
            and (visit_diagnosis_ind = 1 or problem_list_ind = 1)
            and diagnosis_encounter_all.encounter_date <= surgery_procedure.surgery_date
        inner join
            {{ ref('lookup_neuro_dx_grouping')}} as lookup_neuro_dx_grouping
                on diagnosis_encounter_all.icd10_code like lookup_neuro_dx_grouping.dx_cd
        inner join
            {{ ref('lookup_neuroscience_metric_cpt')}} as lookup_neuroscience_metric_cpt
                on (surgery_procedure.cpt_code = cast(lookup_neuroscience_metric_cpt.cpt_code as char(5))
                or surgery_procedure.or_proc_id = cast(lookup_neuroscience_metric_cpt.or_proc_id as char(4)))
                and lookup_neuroscience_metric_cpt.metric = 'convulsive disorders'
    group by
        surgery_procedure.or_key
)

select
    surgery_encounter.csn,
    surgery_encounter.visit_key,
    surgery_encounter.encounter_date,
    surgery_encounter.surgery_date,
    surgery_encounter.mrn,
    surgery_encounter.pat_key,
    surgery_encounter.patient_name,
    surgery_encounter.dob,
    surgery_encounter.surgery_age_years,
    surgery_diagnoses.brain_tumor_enc,
    surgery_diagnoses.epilepsy_enc,
    {{
        dbt_utils.surrogate_key([
            'surgery_encounter.pat_key',
            'surgery_encounter.surgery_date'
        ])
    }} as primary_key
from
    {{ ref('surgery_encounter')}} as surgery_encounter
    inner join surgery_diagnoses on surgery_diagnoses.or_key = surgery_encounter.or_key
where
    lower(surgery_encounter.case_status) = 'completed'
    and surgery_diagnoses.epilepsy_enc = 1
    and surgery_diagnoses.brain_tumor_enc = 0
