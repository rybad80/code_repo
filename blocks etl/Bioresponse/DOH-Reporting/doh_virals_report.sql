/* Final table for IPC to use to report patient respiratory virals to DOH
That report is available through RStudio Dasboard in safety-qualtiy repo
Granularity is one row per visit-infectious-episode*/

with diabetes as (
    select
        diabetes_visit_cohort.patient_key,
        min(diabetes_visit_cohort.endo_vis_dt) as first_diabetes_visit
    from
        {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
    group by
        diabetes_visit_cohort.patient_key
),

liver_failure as (
    select
        usnews_billing.patient_key,
        min(usnews_billing.service_date) as first_liver_failure_visit
    from
        {{ ref('usnews_billing') }} as usnews_billing
    where
        usnews_billing.metric_id = 'd13c'
    group by
        usnews_billing.patient_key
)

select
    -- patient details
    stg_patient.mrn,
    stg_doh_virals_cohort.csn,
    stg_patient.dob,
    stg_patient.patient_name,
    stg_patient.home_phone,
    stg_patient.mailing_address_line1,
    stg_patient.mailing_address_line2,
    stg_patient.mailing_city,
    stg_patient.county,
    stg_patient.mailing_state,
    stg_patient.mailing_zip,
    stg_patient.race_ethnicity,
    stg_patient.sex,
    stg_patient.deceased_ind,
    stg_patient.death_date,
    -- encounter details
    stg_doh_virals_encounter_details.primary_dx_icd,
    stg_doh_virals_encounter_details.primary_dx,
    stg_doh_virals_encounter_details.current_smoker_ind,
    stg_doh_virals_encounter_details.former_smoker_ind,
    stg_doh_virals_encounter_details.pregnant_ind,
    stg_doh_virals_cohort.ip_enter_date,
    stg_doh_virals_encounter_details.discharge_date,
    stg_doh_virals_adt_event.icu_ind,
    stg_doh_virals_encounter_details.patient_age,
    coalesce(diagnosis_medically_complex.resp_ccc_ind, 0) as resp_ccc_ind,
    coalesce(diagnosis_medically_complex.cvd_ccc_ind, 0) as cvd_ccc_ind,
    coalesce(diagnosis_medically_complex.renal_ccc_ind, 0) as renal_ccc_ind,
    coalesce(diagnosis_medically_complex.medically_complex_ind, 0) as medically_complex_ind,
    case when
        liver_failure.first_liver_failure_visit <= stg_doh_virals_cohort.specimen_taken_date
        then 1 else 0
        end as liver_failure_ind,
    case when
        diabetes.first_diabetes_visit <= stg_doh_virals_cohort.specimen_taken_date
        then 1 else 0
        end as diabetes_ind,
    -- viral details
    case when
        stg_doh_virals_cohort.diagnosis_hierarchy_2 is null then diagnosis_hierarchy_1
        else stg_doh_virals_cohort.diagnosis_hierarchy_1 || ' ' || diagnosis_hierarchy_2
    end as bioresponse_full_diagnosis,
    stg_doh_virals_cohort.procedure_name,
    stg_doh_virals_cohort.result_component_name,
    stg_doh_virals_cohort.order_specimen_source,
    stg_doh_virals_cohort.specimen_taken_date as first_collection_date,
    -- flowsheet details
    stg_doh_virals_flowsheet_details.fever_ind,
    stg_doh_virals_flowsheet_details.ecmo_ind,
    stg_doh_virals_flowsheet_details.mechanical_ventilation_ind,
    stg_doh_virals_flowsheet_details.subjective_fever_ind,
    stg_doh_virals_flowsheet_details.chills_ind,
    stg_doh_virals_flowsheet_details.myalgia_ind,
    stg_doh_virals_flowsheet_details.runny_nose_ind,
    stg_doh_virals_flowsheet_details.sore_throat_ind,
    stg_doh_virals_flowsheet_details.cough_ind,
    stg_doh_virals_flowsheet_details.sob_ind,
    stg_doh_virals_flowsheet_details.nausea_vomit_ind,
    stg_doh_virals_flowsheet_details.headache_ind,
    stg_doh_virals_flowsheet_details.abdominal_pain_ind,
    stg_doh_virals_flowsheet_details.diarrhea_ind,
    -- keys
    stg_doh_virals_cohort.encounter_episode_key,
    stg_patient.patient_key,
    stg_doh_virals_cohort.encounter_key
from
    {{ ref('stg_doh_virals_cohort') }} as stg_doh_virals_cohort
    left join {{ ref('stg_doh_virals_encounter_details') }} as stg_doh_virals_encounter_details
        on stg_doh_virals_cohort.encounter_key = stg_doh_virals_encounter_details.encounter_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_doh_virals_cohort.patient_key = stg_patient.patient_key
    left join {{ ref('diagnosis_medically_complex') }} as diagnosis_medically_complex
        on stg_doh_virals_cohort.encounter_key = diagnosis_medically_complex.encounter_key
    left join {{ ref('stg_doh_virals_flowsheet_details') }} as stg_doh_virals_flowsheet_details
        on stg_doh_virals_flowsheet_details.encounter_episode_key = stg_doh_virals_cohort.encounter_episode_key
    left join {{ ref('stg_doh_virals_adt_event') }} as stg_doh_virals_adt_event
        on stg_doh_virals_cohort.encounter_episode_key = stg_doh_virals_adt_event.encounter_episode_key
    left join liver_failure
        on stg_doh_virals_cohort.patient_key = liver_failure.patient_key
    left join diabetes
        on stg_doh_virals_cohort.patient_key = diabetes.patient_key
where
    stg_doh_virals_cohort.order_of_tests = 1
    