with hospital_encounter as (
        select
        encounter_all.visit_key,
        min(ortho_note_all_edits.note_entry_date) as first_ortho_note,
        max(
            case
                when diagnosis_encounter_all.hsp_acct_final_primary_ind = 1 then diagnosis_encounter_all.icd10_code
            end
        ) as icd_code_final,
        max(
            case when diagnosis_encounter_all.ip_admit_primary_ind = 1 then diagnosis_encounter_all.icd10_code end
        ) as icd_code_admit,
        max(
            case when diagnosis_encounter_all.ed_primary_ind = 1 then diagnosis_encounter_all.icd10_code end
        ) as icd_code_ed,
        max(
            case
                when
                    diagnosis_encounter_all.hsp_acct_final_primary_ind = 1
                    then diagnosis_encounter_all.diagnosis_name
            end
        ) as diagnosis_name_final,
        max(
            case
                when diagnosis_encounter_all.ip_admit_primary_ind = 1 then diagnosis_encounter_all.diagnosis_name
            end
        ) as diagnosis_name_admit,
        max(
            case when diagnosis_encounter_all.ed_primary_ind = 1 then diagnosis_encounter_all.diagnosis_name  end
        ) as diagnosis_name_ed,
        count(distinct ortho_note_all_edits.note_key) as n_ortho_notes,
        count(distinct surgery_procedure.case_key) as n_surgeries
        from {{ref('encounter_all')}} as encounter_all
        inner join
            {{ref('ortho_note_all_edits')}} as ortho_note_all_edits on
                ortho_note_all_edits.visit_key = encounter_all.visit_key and edit_seq_number = 1
        left join
            {{ref('surgery_procedure')}} as surgery_procedure on
                surgery_procedure.visit_key = encounter_all.visit_key
                                                                    and lower(
                                                                        surgery_procedure.service
                                                                    ) = 'orthopedics'
        left join
            {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all on
                diagnosis_encounter_all.visit_key = encounter_all.visit_key
    where
        encounter_all.inpatient_ind + encounter_all.ed_ind > 0
    group by
        encounter_all.visit_key
)
select
    hospital_encounter.visit_key,
    encounter_all.patient_name,
    encounter_all.mrn,
    stg_patient.sex,
    stg_patient.race_ethnicity,
    encounter_all.dob,
    encounter_all.csn,
    encounter_all.encounter_date,
    year(add_months(encounter_all.encounter_date, 6)) as fiscal_year,
    year(encounter_all.encounter_date) as calendar_year,
    date_trunc('month', encounter_all.encounter_date) as calendar_month,
    encounter_ed.ed_arrival_date,
    encounter_ed.ed_discharge_date,
    encounter_all.hospital_admit_date,
    encounter_all.hospital_discharge_date,
    encounter_all.patient_class,
    encounter_all.inpatient_ind,
    encounter_all.ed_ind,
    coalesce(encounter_inpatient.admission_type, 'ED only') as admission_type,
    encounter_inpatient.admission_department,
    case
        when encounter_all.inpatient_ind = 0 and encounter_all.ed_ind = 1 then 'ED only'
        when encounter_all.inpatient_ind = 1 and (encounter_all.ed_ind = 1) then 'ED to admission'
        else 'Admission'
    end as hospital_encounter_type,
    coalesce(encounter_inpatient.admission_service, 'ED only') as admission_service,
    case
        when encounter_inpatient.admission_service is not null then encounter_inpatient.discharge_service
        else 'ED only'
    end as discharge_service,
    hospital_encounter.icd_code_final,
    hospital_encounter.icd_code_admit,
    hospital_encounter.icd_code_ed,
    hospital_encounter.diagnosis_name_final,
    hospital_encounter.diagnosis_name_admit,
    hospital_encounter.diagnosis_name_ed,
    hospital_encounter.n_ortho_notes,
    case
        when first_ortho_note between encounter_ed.ed_arrival_date and encounter_ed.ed_discharge_date then 1 else 0
    end as ed_consult_ind,
    hospital_encounter.n_surgeries,
    encounter_inpatient.hospital_los_days,
    encounter_inpatient.inpatient_los_days,
    round(encounter_ed.ed_los_hrs / 60.0, 1) as ed_los_hours,
    encounter_inpatient.icu_los_days,
    encounter_inpatient.icu_ind,
    coalesce(
        encounter_inpatient.medically_complex_ind, encounter_ed.medically_complex_ind
    ) as medically_complex_ind,
    coalesce(
        encounter_inpatient.complex_chronic_condition_ind, encounter_ed.complex_chronic_condition_ind
    ) as complex_chronic_condition_ind,
    encounter_all.pat_key
from
    hospital_encounter
    inner join {{ref('encounter_all')}} as encounter_all on encounter_all.visit_key = hospital_encounter.visit_key
    inner join {{ref('stg_patient')}} as stg_patient on stg_patient.pat_key = encounter_all.pat_key
    left join
        {{ref('encounter_inpatient')}} as encounter_inpatient on
            encounter_inpatient.visit_key = encounter_all.visit_key
    left join {{ref('encounter_ed')}} as encounter_ed on encounter_ed.visit_key = encounter_all.visit_key
where
    hospital_encounter.n_ortho_notes + hospital_encounter.n_surgeries > 0
