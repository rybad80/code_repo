with act_hf_cohort as (
    select
        encounter_all.visit_key,
        encounter_all.csn,
        encounter_all.patient_name,
        encounter_all.mrn,
        encounter_all.encounter_date,
        encounter_all.provider_name,
        encounter_all.provider_id,
        encounter_all.department_name,
        encounter_all.department_id,
        encounter_all.visit_type,
        encounter_all.visit_type_id,
        encounter_all.encounter_type,
        encounter_all.encounter_type_id,
        encounter_all.appointment_status,
        encounter_all.appointment_status_id,
        encounter_all.patient_class,
        encounter_all.hospital_admit_date,
        encounter_all.hospital_discharge_date,
        outpat_enc.specialty_name,
        case when inpat_enc.visit_key is not null then 1 else 0 end as act_hf_inpatient_ind,
        inpat_enc.ccu_ind as ip_ccu_ind,
        inpat_enc.cicu_ind as ip_cicu_ind,
        inpat_enc.consult_hf_prov_ind as ip_consult_by_acthf_ind,
        inpat_enc.fl6_enter_date,
        inpat_enc.currently_in_fl6_ind,
        year(add_months(encounter_all.encounter_date, 6)) as fiscal_year,
        date_trunc('month', encounter_all.encounter_date) as visual_month,
        encounter_all.pat_key,
        encounter_all.hsp_acct_key
    from {{ref('encounter_all')}} as encounter_all
    left join {{ref('stg_frontier_act_hf_inpat_enc')}} as inpat_enc
        on encounter_all.visit_key = inpat_enc.visit_key
    left join {{ref('stg_frontier_act_hf_outpat_enc')}} as outpat_enc
        on encounter_all.visit_key = outpat_enc.visit_key
    where (inpat_enc.visit_key is not null
        or outpat_enc.visit_key is not null
        ) and encounter_all.encounter_date <= current_date
)
select
    act_hf_cohort.visit_key,
    act_hf_cohort.csn,
    act_hf_cohort.patient_name,
    act_hf_cohort.mrn,
    act_hf_cohort.encounter_date,
    act_hf_cohort.provider_name,
    act_hf_cohort.provider_id,
    act_hf_cohort.department_name,
    act_hf_cohort.department_id,
    act_hf_cohort.visit_type,
    act_hf_cohort.visit_type_id,
    act_hf_cohort.encounter_type,
    act_hf_cohort.encounter_type_id,
    act_hf_cohort.appointment_status,
    act_hf_cohort.appointment_status_id,
    act_hf_cohort.patient_class,
    act_hf_cohort.hospital_admit_date,
    act_hf_cohort.hospital_discharge_date,
    act_hf_cohort.specialty_name,
    act_hf_cohort.act_hf_inpatient_ind,
    act_hf_cohort.ip_ccu_ind,
	act_hf_cohort.ip_cicu_ind,
	act_hf_cohort.ip_consult_by_acthf_ind,
    act_hf_cohort.fl6_enter_date,
	act_hf_cohort.currently_in_fl6_ind,
    pat_base.vad_pat_ind,
    act_hf_cohort.fiscal_year,
    act_hf_cohort.visual_month,
    act_hf_cohort.pat_key,
    act_hf_cohort.hsp_acct_key
from {{ref('stg_frontier_act_hf_pat_base')}} as pat_base
inner join act_hf_cohort as act_hf_cohort
    on act_hf_cohort.mrn = pat_base.mrn
