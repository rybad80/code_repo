with primary_dx as (
    select
        stg_encounter_inpatient.visit_key,
        --'hsp acct final - primary'
        max(diagnosis.dx_nm) as primary_dx,
        -- 'hsp acct final - primary'
        max(snomed_concept.fully_specified_nm) as primary_dx_snomed,
        --'hsp acct final - primary',
        max(diagnosis.icd10_cd) as primary_dx_icd
    from
        {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        inner join {{source('cdw', 'visit_diagnosis')}} as visit_diagnosis
            on visit_diagnosis.visit_key = stg_encounter_inpatient.visit_key
            and visit_diagnosis.dict_dx_sts_key = 321
        left join {{source('cdw', 'diagnosis')}} as diagnosis
            on diagnosis.dx_key = visit_diagnosis.dx_key
        left join {{source('cdw', 'diagnosis_clinical_concept_snomed')}} as diagnosis_clinical_concept_snomed
            on visit_diagnosis.dx_key = diagnosis_clinical_concept_snomed.dx_key
        left join {{source('cdw', 'snomed_concept')}} as snomed_concept
            on diagnosis_clinical_concept_snomed.snomed_concept_key = snomed_concept.snomed_concept_key
        group by
        stg_encounter_inpatient.visit_key
),

transport_admissions as (
select
	stg_encounter.visit_key
from
	{{ref('stg_transport_new_calls')}} as stg_transport_new_calls
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.pat_key = stg_transport_new_calls.pat_key
        and stg_encounter.hospital_admit_date >= stg_transport_new_calls.intake_date
        and stg_encounter.hospital_admit_date <= coalesce(
                stg_transport_new_calls.transport_complete_canceled_date,
                current_date)
where
    lower(transport_type_raw) like '%inbound%'
    and lower(transport_type_raw) not like '%interfacility%'
    and lower(final_status) = 'completed'
group by
    stg_encounter.visit_key

union all

select
	stg_encounter.visit_key
from
	{{ref('stg_transport_historic_calls')}} as stg_transport_historic_calls
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.pat_key = stg_transport_historic_calls.pat_key
        and stg_encounter.hospital_admit_date >= stg_transport_historic_calls.intake_date
        and stg_encounter.hospital_admit_date <= coalesce(
                stg_transport_historic_calls.transport_complete_canceled_date,
                current_date)
where
    lower(transport_type_raw) like '%inbound%'
    and lower(transport_type_raw) not like '%interfacility%'
    and lower(final_status) = 'completed'
group by
    stg_encounter.visit_key
)

select
    stg_encounter_inpatient.visit_key,
    stg_encounter.encounter_key,
    stg_patient_ods.patient_name,
    stg_patient_ods.mrn,
    stg_patient_ods.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter_inpatient.ip_enter_date as inpatient_admit_date,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date,
    stg_patient_ods.sex,
    stg_patient_ods.gestational_age_complete_weeks,
    stg_encounter.age_years,
    stg_encounter.age_days,
    stg_encounter.admission_type,
    stg_encounter_inpatient.admission_service,
    stg_hsp_acct_xref.hsp_acct_patient_class,
    case
        when stg_encounter.hospital_discharge_date is null
        then 1
        else 0
    end as currently_admitted_ind,
    extract( --noqa: PRS
        epoch from stg_encounter.hospital_discharge_date - stg_encounter.hospital_admit_date
    ) / 86400.0 as hospital_los_days,
    extract( --noqa: PRS
        epoch from stg_encounter.hospital_discharge_date - stg_encounter_inpatient.ip_enter_date
    ) / 86400.0 as inpatient_los_days,
    stg_encounter_inpatient.admission_department_group as admission_department,
    admission_department_center_id,
    admission_department_center_abbr,
    disch_prov.full_nm as discharge_provider_name,
    dict_dischrg_dspn.dict_nm as discharge_disposition,
    case
      when lower(stg_encounter_inpatient.admission_department_group) = 'sdu'
        and stg_encounter.age_years > 5 then 'SDU Birth Parent'
      when lower(stg_encounter_inpatient.admission_department_group) = 'sdu'
        and stg_encounter.age_days <= 5 then 'SDU Neonate'
      when lower(stg_encounter.admission_type) = 'elective' then 'Elective'
      when stg_encounter_inpatient.ed_ind = 1 then 'ED'
      when transport_admissions.visit_key is not null then 'Transfer'
      else 'Direct Admission'
    end as admission_source,
    stg_encounter_inpatient.discharge_service,
    stg_encounter_inpatient.discharge_department_group as discharge_department,
    discharge_department_center_id,
    discharge_department_center_abbr,
    primary_dx.primary_dx,
    primary_dx.primary_dx_snomed,
    primary_dx.primary_dx_icd,
    stg_encounter.patient_address_seq_num,
    stg_encounter.patient_address_zip_code,
    stg_encounter_payor.payor_name,
    stg_encounter_payor.payor_group,
    stg_encounter_inpatient.ed_ind,
    stg_encounter_inpatient.icu_ind,
    stg_encounter_inpatient.icu_los_days,
    stg_diagnosis_medically_complex.complex_chronic_condition_ind,
    stg_diagnosis_medically_complex.medically_complex_ind,
    stg_diagnosis_medically_complex.tech_dependent_ind,
    stg_encounter.walkin_ind,
    stg_encounter.online_scheduled_ind,
    stg_encounter.pat_key,
    stg_encounter.patient_key,
    stg_patient_ods.pat_id,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    stg_hsp_acct_xref.hsp_account_id,
    -- admission visit_event_key used in patient flow
    visit_event.visit_event_key as admission_event_key,
    stg_encounter_inpatient.admission_dept_key,
    stg_encounter_inpatient.discharge_dept_key
from
    {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = stg_encounter_inpatient.visit_key
    inner join {{ref('stg_patient_ods')}} as stg_patient_ods
        on stg_patient_ods.patient_key = stg_encounter.patient_key
    inner join {{ref('stg_diagnosis_medically_complex')}} as stg_diagnosis_medically_complex
        on stg_diagnosis_medically_complex.visit_key = stg_encounter_inpatient.visit_key
    inner join {{source('cdw','visit_event')}} as visit_event
        on visit_event.adt_event_id = stg_encounter_inpatient.event_id
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter_inpatient.visit_key
    inner join {{source('cdw', 'visit_addl_info')}} as visit_addl_info
        on visit_addl_info.visit_key = stg_encounter_inpatient.visit_key
    left join primary_dx
        on primary_dx.visit_key = stg_encounter_inpatient.visit_key
    left join {{source('cdw', 'provider')}} as admit_prov
        on admit_prov.prov_key = visit_addl_info.admit_prov_key
    left join {{source('cdw', 'provider_specialty')}} as admit_prov_spec
        on admit_prov_spec.prov_key = admit_prov.prov_key and admit_prov_spec.line = 1
    left join {{source('cdw', 'cdw_dictionary')}} as dict_dischrg_dspn
        on dict_dischrg_dspn.dict_key = visit_addl_info.dict_dischrg_dspn_key
    left join {{source('cdw', 'provider')}} as disch_prov
        on disch_prov.prov_key = visit_addl_info.dischrg_prov_key
    left join {{source('cdw', 'provider_specialty')}} as disch_prov_spec
        on disch_prov_spec.prov_key = disch_prov.prov_key and disch_prov_spec.line = 1
    left join transport_admissions
        on transport_admissions.visit_key = stg_encounter.visit_key
