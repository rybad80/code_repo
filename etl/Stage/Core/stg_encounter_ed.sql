{{ config(
    materialized='table', dist='visit_key',
    meta={
        'critical': true
    }
) }}

with ed_diagnosis_raw as (
    select
        stg_encounter.encounter_key,
        dim_diagnosis.current_icd10_code as icd10_cd,
        dim_diagnosis.current_icd9_code as icd9_cd,
        dim_diagnosis.diagnosis_id as dx_id,
        dim_diagnosis.diagnosis_name as dx_nm,
        max(case when dx_status = 'ED Primary' then 1 else 0 end)
            over(partition by stg_encounter.encounter_key) as ed_primary_dx_visit_ind,
        max(case when dx_status = 'Visit Primary' then 1 else 0 end)
            over(partition by stg_encounter.encounter_key) as visit_primary_dx_visit_ind,
        case when dx_status = 'ED Primary' then 1 else 0 end as ed_primary_ind,
        case when dx_status = 'ED Other' then 1 else 0 end as ed_other_ind,
        case when dx_status = 'Visit Primary' then 1 else 0 end as visit_primary_ind,
        case when dx_status = 'Visit Other' then 1 else 0 end as visit_other_ind,
        case when src = 'HSP_ACCT_ADMIT_DX' and line = 1 then 1 else 0 end as hsp_acct_admit_primary_ind,
        stg_dx_visit_diagnosis.line as seq_num
    from
        {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = fact_edqi.visit_key
        inner join {{ref('stg_dx_visit_diagnosis_long')}} as stg_dx_visit_diagnosis
            on stg_dx_visit_diagnosis.pat_enc_csn_id = stg_encounter.csn
        inner join {{ref('dim_diagnosis')}} as dim_diagnosis
            on dim_diagnosis.diagnosis_id = stg_dx_visit_diagnosis.dx_id
),

ed_diagnosis as (
    select
        encounter_key,
        max(case
                when ed_primary_dx_visit_ind = 1 and ed_primary_ind = 1
                    then ed_diagnosis_raw.icd10_cd
                when ed_primary_dx_visit_ind = 0 and ed_other_ind = 1 and seq_num = 1
                    then ed_diagnosis_raw.icd10_cd
                else null end) as clinical_dx_primary_icd10,
        max(case
                when ed_primary_dx_visit_ind = 1 and ed_primary_ind = 1
                    then ed_diagnosis_raw.icd9_cd
                when ed_primary_dx_visit_ind = 0 and ed_other_ind = 1 and seq_num = 1
                    then ed_diagnosis_raw.icd9_cd
                else null end) as clinical_dx_primary_icd9,
        max(case
                when ed_primary_dx_visit_ind = 1 and ed_primary_ind = 1
                    then dx_key
                when ed_primary_dx_visit_ind = 0 and ed_other_ind = 1 and seq_num = 1
                    then dx_key
                else null end) as clinical_dx_primary_dx_key,
        group_concat(
            case when ed_primary_ind + ed_other_ind > 0 then ed_diagnosis_raw.dx_nm else null end, ';'
        ) as clinical_dx_all_dx_nm,
        max(case
                when visit_primary_dx_visit_ind = 1 and visit_primary_ind = 1
                    then ed_diagnosis_raw.icd10_cd
                when visit_primary_dx_visit_ind = 0 and visit_other_ind = 1 and seq_num = 1
                    then ed_diagnosis_raw.icd10_cd
                else null end) as visit_dx_primary_icd10,
        max(case
                when visit_primary_dx_visit_ind = 1 and visit_primary_ind = 1
                    then ed_diagnosis_raw.icd9_cd
                when visit_primary_dx_visit_ind = 0 and visit_other_ind = 1 and seq_num = 1
                    then ed_diagnosis_raw.icd9_cd
                else null end) as visit_dx_primary_icd9,
        max(case
                when visit_primary_dx_visit_ind = 1 and visit_primary_ind = 1
                    then dx_key
                when visit_primary_dx_visit_ind = 0 and visit_other_ind = 1 and seq_num = 1
                    then dx_key
                else null end) as visit_dx_primary_dx_key,
        group_concat(
            case when visit_primary_ind + visit_other_ind > 0 then ed_diagnosis_raw.dx_nm else null end, ';'
        ) as visit_dx_all_dx_nm,
        max(
            case when hsp_acct_admit_primary_ind = 1 then ed_diagnosis_raw.icd10_cd else null end
        ) as billing_dx_primary_icd10,
        max(
            case when hsp_acct_admit_primary_ind = 1 then ed_diagnosis_raw.icd9_cd else null end
        ) as billing_dx_primary_icd9,
        max(case when hsp_acct_admit_primary_ind = 1 then dx_key else null end) as billing_dx_primary_dx_key
    from
        ed_diagnosis_raw
        left join {{source('cdw','master_diagnosis')}} as master_diagnosis
            on master_diagnosis.dx_id = ed_diagnosis_raw.dx_id
    group by
        encounter_key
),


adt_bed as (
    select
        fact_edqi.visit_key,
        max(
            case when lower(clarity_bed.bed_label) like 'ec%' then 1 else 0 end
        ) as edecu_bed_ind
    from
        {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
        inner join {{source('clarity_ods','clarity_adt')}} as clarity_adt
            on clarity_adt.pat_enc_csn_id = fact_edqi.enc_id
        inner join {{source('clarity_ods','clarity_bed')}} as clarity_bed
            on clarity_bed.bed_id = clarity_adt.bed_id
    group by
        fact_edqi.visit_key
),

primary_reason_for_visit as (
    select
        fact_edqi.visit_key,
        cl_rsn_for_visit.reason_visit_name as rsn_nm,
        cl_rsn_for_visit.reason_visit_id as rsn_id
    from
        {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = fact_edqi.visit_key
        inner join {{source('clarity_ods','pat_enc_rsn_visit')}} as pat_enc_rsn_visit
            on pat_enc_rsn_visit.pat_enc_csn_id = stg_encounter.csn
        inner join {{source('clarity_ods','cl_rsn_for_visit')}} as cl_rsn_for_visit
            on cl_rsn_for_visit.reason_visit_id = pat_enc_rsn_visit.enc_reason_id
    where
        pat_enc_rsn_visit.line = 1
),

combine_md_eval_dates as (
    select
        fact_edqi.visit_key,
        min(fact_edqi.md_evaluation_dt) as min_md_evaluation_date,
        min(fact_edqi.earliest_md_eval_dt) as min_earliest_md_eval_dt,
        case
            when min_md_evaluation_date < min_earliest_md_eval_dt then min_md_evaluation_date --noqa: L028
            when min_earliest_md_eval_dt is null then min_md_evaluation_date     --noqa: L028
            else  min_earliest_md_eval_dt --noqa: L028
        end as md_evaluation_date
    from {{source('cdw_analytics','fact_edqi')}} as fact_edqi
    group by
        fact_edqi.visit_key
)
select
    fact_edqi.visit_key,
    stg_encounter.encounter_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.sex,
    stg_encounter.age_years,
    stg_encounter.age_days,
    fact_edqi.arrive_ed_dt as ed_arrival_date,
    fact_edqi.triage_start_dt as ed_triage_start_date,
    fact_edqi.roomed_ed_dt as ed_roomed_date,
    combine_md_eval_dates.md_evaluation_date,
    fact_edqi.disch_ed_dt as ed_discharge_date,
    fact_edqi.admit_edecu_dt as edecu_admit_date,
    fact_edqi.disch_edecu_dt as edecu_discharge_date,
    fact_edqi.ed_los / 60.0 as ed_los_hrs,
    fact_edqi.edecu_los / 60.0 as edecu_los_hrs,
    fact_edqi.initial_ed_department_center_id,
    fact_edqi.initial_ed_department_center_abbr,
    fact_edqi.final_ed_department_center_id,
    fact_edqi.final_ed_department_center_abbr,
    coalesce(
        ed_diagnosis.clinical_dx_primary_icd10, ed_diagnosis.visit_dx_primary_icd10
    ) as clinical_dx_primary_icd10,
    coalesce(
        ed_diagnosis.clinical_dx_primary_icd9, ed_diagnosis.visit_dx_primary_icd9
    ) as clinical_dx_primary_icd9,
    coalesce(ed_diagnosis.clinical_dx_all_dx_nm, ed_diagnosis.visit_dx_all_dx_nm) as clinical_dx_all_dx_nm,
    ed_diagnosis.billing_dx_primary_icd9,
    ed_diagnosis.billing_dx_primary_icd10,
    primary_reason_for_visit.rsn_nm as primary_reason_for_visit_name,
    primary_reason_for_visit.rsn_id as primary_reason_for_visit_id,
    dict_acuity.dict_nm as acuity_esi,
    case when stg_encounter_inpatient.visit_key is not null then 1 else 0 end as inpatient_ind,
    stg_encounter_inpatient.ip_enter_date as inpatient_admit_date,
    stg_encounter_inpatient.admission_department_group as admission_department_name,
    stg_encounter.hospital_discharge_date,
    case when fact_edqi.edecu_ind = 1 and adt_bed.edecu_bed_ind = 1 then 1 else 0 end as edecu_ind,
    coalesce(stg_encounter_inpatient.icu_ind, 0) as icu_ind,
    fact_edqi.hr_72_revisit_first_visit_ind as revisit_72_hour_ind,
    stg_encounter.patient_address_seq_num,
    stg_encounter.patient_address_zip_code,
    stg_patient_pcp_attribution.pcp_location as primary_care_location,
    stg_ed_core_flowsheet.ed_visit_language,
    stg_ed_core_flowsheet.ed_visit_language_comment,
    ed_diagnosis.billing_dx_primary_dx_key,
    coalesce(
        ed_diagnosis.clinical_dx_primary_dx_key, ed_diagnosis.visit_dx_primary_dx_key
    ) as clinical_dx_primary_dx_key,
    stg_diagnosis_medically_complex.complex_chronic_condition_ind,
    stg_diagnosis_medically_complex.medically_complex_ind,
    stg_diagnosis_medically_complex.tech_dependent_ind,
    stg_encounter.pat_key,
    stg_encounter.patient_key,
    stg_encounter.prov_key,
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.department_id,
    fact_edqi.ed_patients_seen_ind
from
    {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = fact_edqi.visit_key
    left join ed_diagnosis
        on ed_diagnosis.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = fact_edqi.visit_key
    left join {{ref('stg_diagnosis_medically_complex')}} as stg_diagnosis_medically_complex
        on stg_diagnosis_medically_complex.visit_key = fact_edqi.visit_key
    left join adt_bed
        on adt_bed.visit_key = fact_edqi.visit_key
    left join primary_reason_for_visit
        on primary_reason_for_visit.visit_key = fact_edqi.visit_key
    left join combine_md_eval_dates
        on combine_md_eval_dates.visit_key = fact_edqi.visit_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_acuity
        on dict_acuity.dict_key = fact_edqi.dict_acuity_key
    left join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key = fact_edqi.initial_ed_dept_key
    left join {{ref('stg_patient_pcp_attribution')}} as stg_patient_pcp_attribution
        on fact_edqi.pat_key = stg_patient_pcp_attribution.pat_key
           and fact_edqi.arrive_ed_dt::date between stg_patient_pcp_attribution.start_date
                                                    and stg_patient_pcp_attribution.end_date
    left join {{ref('stg_ed_core_flowsheet')}} as stg_ed_core_flowsheet
      on fact_edqi.visit_key = stg_ed_core_flowsheet.visit_key
