with stage as (
    select
        surgery_procedure.or_key,
        surgery_procedure.csn,
        surgery_procedure.visit_key,
        surgery_procedure.mrn,
        surgery_procedure.pat_key,
        surgery_procedure.patient_name,
        surgery_procedure.encounter_date,
        encounter_inpatient.hospital_discharge_date,
        encounter_inpatient.inpatient_los_days,
        encounter_inpatient.icu_los_days,
        surgery_procedure.post_op_los_days as post_op_los_days,
        surgery_procedure.surgery_date as surgery_date,
        procedure_seq_num,
        surgery_procedure.cpt_code,
        surgery_procedure.or_proc_id,
        case
            when encounter_inpatient.visit_key is not null
                then 1
                else 0
            end as ip_ind,
        /* Used to find the first surgery date for the encounter. Ordered this way because there are some
        instances where there's no procedure_seq_num = 1 for the visit This ordering will assign the procedure
        with the lowest seq_num to the same row as surgery_day_row */
        row_number() over (partition by surgery_procedure.visit_key
            order by surgery_procedure.surgery_date, procedure_seq_num, post_op_los_days desc) as surgery_day_row
    from {{ ref('surgery_procedure')}} as surgery_procedure
        left join {{ ref('encounter_inpatient')}} as encounter_inpatient
            on surgery_procedure.visit_key = encounter_inpatient.visit_key
    where
        lower(surgery_procedure.service) in ('neurosurgery', 'neuro surgery')
        and lower(surgery_procedure.case_status) = 'completed'
),

surgical_dx as (
    select
        or_key,
        case
            when instr(lower(note_text.note_text), 'diagnosis:') != 0  --if this text string exist in note text
                then substring(note_text.note_text,
                    instr(lower(note_text.note_text), 'diagnosis:'), 150) --150 characters from "diagnosis:"
            when instr(lower(note_text.note_text), 'diagnoses:') != 0 --if this text string exist in note text
                then substring(note_text.note_text,
                    instr(lower(note_text.note_text), 'diagnoses:'), 150) --150 characters from "diagnoses:"
            else null
        end as diagnosis_text
    from
        stage
    inner join
        {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
        on stage.visit_key = note_edit_metadata_history.visit_key
        and date(stage.surgery_date) = date(note_edit_metadata_history.service_date)
    inner join
        {{source('cdw', 'note_text')}} as note_text
        on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
    where
        (
        lower(note_text.note_text) like '%diagnoses%'
            or lower(note_text.note_text) like '%diagnosis%') --limiting # notes in CTE
        and note_type_id in ('400004', '400007') --post operative note ids
        and lower(note_text.note_text) like '%epilepsy%' --limiting # notes in CTE
)
select
    stage.or_key,
    stage.csn,
    stage.visit_key,
    stage.mrn,
    stage.pat_key,
    stage.patient_name,
    stage.encounter_date,
    stage.hospital_discharge_date,
    stage.inpatient_los_days,
    stage.icu_los_days,
    stage.post_op_los_days,
    stage.surgery_date,
    stage.procedure_seq_num,
    stage.cpt_code,
    stage.or_proc_id,
    stage.ip_ind,
    stage.surgery_day_row,
    max(
        case
            when lower(diagnosis_text) like '%epilepsy%'
            then 1
            else 0
            end) as epilepsy_surgical_dx_ind
from
    stage
left join
    surgical_dx
    on stage.or_key = surgical_dx.or_key
group by
    stage.or_key,
    stage.csn,
    stage.visit_key,
    stage.mrn,
    stage.pat_key,
    stage.patient_name,
    stage.encounter_date,
    stage.hospital_discharge_date,
    stage.inpatient_los_days,
    stage.icu_los_days,
    stage.post_op_los_days,
    stage.surgery_date,
    stage.procedure_seq_num,
    stage.cpt_code,
    stage.or_proc_id,
    stage.ip_ind,
    stage.surgery_day_row
