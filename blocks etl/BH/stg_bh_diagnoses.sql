select
    diagnosis_encounter_all.visit_key,
    diagnosis_encounter_all.dx_key,
    diagnosis_encounter_all.pat_key,
    diagnosis_encounter_all.mrn,
    diagnosis_encounter_all.csn,
    diagnosis_encounter_all.encounter_date,
    diagnosis_encounter_all.diagnosis_name,
    diagnosis_encounter_all.icd10_code,
    diagnosis_encounter_all.source_summary,
    diagnosis_encounter_all.visit_diagnosis_ind,
    diagnosis_encounter_all.visit_diagnosis_seq_num,
    diagnosis_encounter_all.marked_primary_ind,
    diagnosis_encounter_all.ed_primary_ind,
    diagnosis_encounter_all.ed_other_ind,
    diagnosis_encounter_all.problem_list_ind,
    diagnosis_encounter_all.problem_noted_date,
    diagnosis_encounter_all.problem_resolved_date
from
    {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
where
    --- CHOP ALL BEHAVIORAL HEALTH DIAGNOSES
    exists (
        select grouper_compiled_rec_list.grouper_records_numeric_id as diagnosis_id
        from {{source('clarity_ods', 'grouper_compiled_rec_list')}} as grouper_compiled_rec_list
        where
            grouper_compiled_rec_list.base_grouper_id = '118797'
            and diagnosis_encounter_all.diagnosis_id = grouper_compiled_rec_list.grouper_records_numeric_id
    )
    and diagnosis_encounter_all.encounter_date >= '2018-01-01'
    and diagnosis_encounter_all.source_summary not like '%pb%'
