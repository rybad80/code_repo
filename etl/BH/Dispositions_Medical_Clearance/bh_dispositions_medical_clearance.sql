with combined_data as (
select
    coalesce(
        stg_bh_mh_evals.visit_key,
        stg_sw_dispos.visit_key,
        stg_bh_medically_cleared.visit_key)
        as visit_key,
    min(stg_bh_mh_evals.ed_dispo_first) as ed_dispo_first,
    min(stg_bh_mh_evals.ed_dispo_first_time) as ed_dispo_first_time,
    min(stg_bh_mh_evals.ed_dispo_last) as ed_dispo_last,
    min(stg_bh_mh_evals.ed_dispo_last_time) as ed_dispo_last_time,
    min(stg_bh_mh_evals.ed_dispo_first_ip_ind) as ed_dispo_first_ip_ind,
    min(stg_bh_mh_evals.ed_dispo_last_ip_ind) as ed_dispo_last_ip_ind,
    min(stg_bh_mh_evals.ed_dispo_last_eating_ind) as ed_dispo_last_eating_ind,
    min(stg_bh_mh_evals.ed_dispo_last_substance_ind) as ed_dispo_last_substance_ind,
    min(stg_bh_mh_evals.ed_dispo_last_wrap_ind) as ed_dispo_last_wrap_ind,
    min(stg_bh_mh_evals.ed_dispo_last_php_ind) as ed_dispo_last_php_ind,
    min(stg_bh_mh_evals.ed_dispo_last_op_psychotherapy_ind) as ed_dispo_last_op_psychotherapy_ind,
    min(stg_bh_mh_evals.ed_dispo_last_op_psychiatry_ind) as ed_dispo_last_op_psychiatry_ind,
    min(stg_bh_mh_evals.ed_dispo_last_cmis_ind) as ed_dispo_last_cmis_ind,
    min(stg_bh_mh_evals.ed_dispo_last_none) as ed_dispo_last_none,
    min(stg_bh_mh_evals.ed_dispo_last_deferred) as ed_dispo_last_deferred,
    min(stg_bh_mh_evals.ed_dispo_last_other) as ed_dispo_last_other,
    min(stg_bh_mh_evals.eating_disorder_dispo_first) as eating_disorder_dispo_first,
    min(stg_bh_mh_evals.eating_disorder_dispo_first_time) as eating_disorder_dispo_first_time,
    min(stg_bh_mh_evals.eating_disorder_dispo_last) as eating_disorder_dispo_last,
    min(stg_bh_mh_evals.eating_disorder_dispo_last_time) as eating_disorder_dispo_last_time,
    min(stg_sw_dispos.sw_placement_status) as sw_placement_status,
    min(stg_sw_dispos.sw_dispo_up_to_date) as sw_dispo_up_to_date,
    min(stg_sw_dispos.sw_dispo_problems) as sw_dispo_problems,
    min(stg_sw_dispos.problems_aggression_ind) as problems_aggression_ind,
    min(stg_sw_dispos.problems_asd_ind) as problems_asd_ind,
    min(stg_sw_dispos.problems_eating_ind) as problems_eating_ind,
    min(stg_sw_dispos.problems_elopement_ind) as problems_elopement_ind,
    min(stg_sw_dispos.problems_ingestion_ind) as problems_ingestion_ind,
    min(stg_sw_dispos.problems_other_ind) as problems_other_ind,
    min(stg_sw_dispos.problems_si_ind) as problems_si_ind,
    min(stg_sw_dispos.complex_dispo_ind) as complex_dispo_ind,
    min(stg_sw_dispos.complex_dispo_med_history_ind) as complex_dispo_med_history_ind,
    min(stg_sw_dispos.complex_dispo_dhs_ind) as complex_dispo_dhs_ind,
    min(stg_sw_dispos.complex_dispo_med_equip_ind) as complex_dispo_med_equip_ind,
    min(stg_sw_dispos.complex_dispo_other_ind) as complex_dispo_other_ind,
    min(stg_sw_dispos.complex_dispo_rtf_ind) as complex_dispo_rtf_ind,
    min(stg_sw_dispos.complex_dispo_sex_ind) as complex_dispo_sex_ind,
    min(stg_sw_dispos.sw_final_dispo) as sw_final_dispo,
    min(stg_sw_dispos.sw_final_dispo_ip_ind) as sw_final_dispo_ip_ind,
    min(stg_sw_dispos.sw_final_dispo_op_con_ind) as sw_final_dispo_op_con_ind,
    min(stg_sw_dispos.sw_final_dispo_op_ind) as sw_final_dispo_op_ind,
    min(stg_sw_dispos.sw_final_dispo_rtf_ind) as sw_final_dispo_rtf_ind,
    min(stg_sw_dispos.sw_final_dispo_php_ind) as sw_final_dispo_php_ind,
    min(stg_sw_dispos.sw_final_dispo_crr_ind) as sw_final_dispo_crr_ind,
    min(stg_sw_dispos.sw_final_dispo_no_int_ind) as sw_final_dispo_no_int_ind,
    min(stg_sw_dispos.sw_final_dispo_ip_sites) as sw_final_dispo_ip_sites,
    min(stg_sw_dispos.sw_final_dispo_op_sites) as sw_final_dispo_op_sites,
    min(stg_sw_dispos.sw_final_dispo_iop_sites) as sw_final_dispo_iop_sites,
    min(stg_sw_dispos.sw_final_dispo_php_sites) as sw_final_dispo_php_sites,
    min(stg_sw_dispos.sw_final_dispo_rtf_sites) as sw_final_dispo_rtf_sites,
    min(stg_bh_medically_cleared.old_sde_mc_date) as old_sde_mc_date,
    min(stg_bh_medically_cleared.sw_form_mc_date_first) as sw_form_mc_date_first,
    min(stg_bh_medically_cleared.order_mc_yes_date_time_first) as order_mc_yes_date_time_first,
    min(stg_bh_medically_cleared.order_mc_yes_date_time_last) as order_mc_yes_date_time_last,
    min(stg_bh_medically_cleared.order_mc_status_last) as order_mc_status_last,
    min(stg_bh_medically_cleared.order_mc_expected_time_frame) as order_mc_expected_time_frame,
    min(stg_bh_medically_cleared.mc_date_earliest) as mc_date_earliest,
    min(stg_bh_medically_cleared.sw_form_first_discharge_complete) as sw_form_first_discharge_complete
from
    {{ref('stg_bh_mh_evals')}} as stg_bh_mh_evals
    full join {{ref('stg_sw_dispos')}} as stg_sw_dispos
        on stg_sw_dispos.visit_key = stg_bh_mh_evals.visit_key
    full join {{ref('stg_bh_medically_cleared')}} as stg_bh_medically_cleared
        on stg_bh_medically_cleared.visit_key = stg_bh_mh_evals.visit_key
group by
    1
)

select
    combined_data.visit_key,
    combined_data.ed_dispo_first,
    combined_data.ed_dispo_first_time,
    combined_data.ed_dispo_last,
    combined_data.ed_dispo_last_time,
    combined_data.ed_dispo_first_ip_ind,
    combined_data.ed_dispo_last_ip_ind,
    combined_data.ed_dispo_last_eating_ind,
    combined_data.ed_dispo_last_substance_ind,
    combined_data.ed_dispo_last_wrap_ind,
    combined_data.ed_dispo_last_php_ind,
    combined_data.ed_dispo_last_op_psychotherapy_ind,
    combined_data.ed_dispo_last_op_psychiatry_ind,
    combined_data.ed_dispo_last_cmis_ind,
    combined_data.ed_dispo_last_none,
    combined_data.ed_dispo_last_deferred,
    combined_data.ed_dispo_last_other,
    combined_data.eating_disorder_dispo_first,
    combined_data.eating_disorder_dispo_first_time,
    combined_data.eating_disorder_dispo_last,
    combined_data.eating_disorder_dispo_last_time,
    combined_data.problems_aggression_ind,
    combined_data.problems_asd_ind,
    combined_data.problems_eating_ind,
    combined_data.problems_elopement_ind,
    combined_data.problems_ingestion_ind,
    combined_data.problems_si_ind,
    combined_data.problems_other_ind,
    combined_data.complex_dispo_ind,
    combined_data.complex_dispo_med_history_ind,
    combined_data.complex_dispo_med_equip_ind,
    combined_data.complex_dispo_dhs_ind,
    combined_data.complex_dispo_other_ind,
    combined_data.complex_dispo_rtf_ind,
    combined_data.complex_dispo_sex_ind,
    combined_data.sw_final_dispo,
    combined_data.sw_final_dispo_ip_ind,
    combined_data.sw_final_dispo_op_con_ind,
    combined_data.sw_final_dispo_op_ind,
    combined_data.sw_final_dispo_rtf_ind,
    combined_data.sw_final_dispo_php_ind,
    combined_data.sw_final_dispo_crr_ind,
    combined_data.sw_final_dispo_no_int_ind,
    combined_data.sw_final_dispo_ip_sites,
    combined_data.sw_final_dispo_op_sites,
    combined_data.sw_final_dispo_iop_sites,
    combined_data.sw_final_dispo_php_sites,
    combined_data.sw_final_dispo_rtf_sites,
    combined_data.sw_placement_status,
    combined_data.sw_dispo_up_to_date,
    combined_data.old_sde_mc_date,
    combined_data.sw_form_mc_date_first,
    combined_data.order_mc_yes_date_time_first,
    combined_data.order_mc_yes_date_time_last,
    combined_data.order_mc_status_last,
    combined_data.order_mc_expected_time_frame,
    combined_data.mc_date_earliest,
    combined_data.sw_form_first_discharge_complete,
    combined_data.sw_dispo_problems,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date,
    combined_data.mc_date_earliest - date(stg_encounter.hospital_admit_date)
        as admit_to_mc_days,
    date(stg_encounter.hospital_discharge_date) - combined_data.mc_date_earliest
        as mc_to_discharge_days,
    encounter_inpatient.hospital_los_days,
    encounter_inpatient.inpatient_los_days,
    stg_encounter.patient_class,
    stg_encounter_payor.payor_group,
    stg_encounter.patient_name,
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.encounter_date,
    stg_encounter.csn,
    stg_encounter_ed.acuity_esi,
    case
        when encounter_inpatient.visit_key is not null
        then 1 else 0
    end as inpatient_ind,
    case
        when stg_encounter_ed.visit_key is not null
        then 1 else 0
    end as ed_ind,
    stg_encounter_ed.edecu_ind,
    stg_encounter_ed.icu_ind,
    encounter_inpatient.admission_source,
    encounter_inpatient.admission_service,
    encounter_inpatient.admission_department,
    encounter_inpatient.admission_department_center_abbr,
    encounter_inpatient.discharge_disposition,
    encounter_inpatient.discharge_service,
    encounter_inpatient.discharge_department,
    encounter_inpatient.discharge_department_center_abbr
from
    combined_data
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = combined_data.visit_key
    left join {{ref('stg_encounter_ed')}} as stg_encounter_ed
        on stg_encounter_ed.visit_key = combined_data.visit_key
    left join {{ref('encounter_inpatient')}} as encounter_inpatient
        on encounter_inpatient.visit_key = combined_data.visit_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter.visit_key
