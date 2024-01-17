select
    fact_edqi.visit_key,
    fact_edqi.edecu_arrvl_dt as edecu_arrive_date,
    fact_edqi.arrive_ed_dt as ed_arrive_date,
    fact_edqi.admit_ed_dt as ed_admit_date,
    fact_edqi.disch_ed_dt as ed_discharge_date,
    fact_edqi.admit_edecu_dt as edecu_admit_date,
    fact_edqi.disch_edecu_dt as edecu_discharge_date,
    fact_edqi.depart_ed_dt as ed_depart_date,
    fact_edqi.triage_start_dt as triage_start_date,
    fact_edqi.triage_end_dt as triage_end_date,
    fact_edqi.assign_rn_dt as assign_rn_date,
    fact_edqi.assign_resident_np_dt as assign_resident_np_date,
    fact_edqi.assign_1st_attending_dt as assign_first_attending_date,
    fact_edqi.registration_start_dt as registration_start_date,
    fact_edqi.roomed_ed_dt as ed_roomed_date,
    fact_edqi.registration_end_dt as registration_end_date,
    fact_edqi.ed_conference_review_dt as ed_conference_review_date,
    fact_edqi.md_evaluation_dt as md_evaluation_date,
    fact_edqi.attending_evaluation_dt as attending_evaluation_date,
    fact_edqi.after_visit_summary_printed_dt as after_visit_summary_printed_date,
    fact_edqi.md_report_dt as md_report_date,
    fact_edqi.paged_ip_rn_dt as paged_ip_rn_date,
    fact_edqi.paged_ip_md_dt as paged_ip_md_date,
    fact_edqi.ip_bed_assigned_dt as ip_bed_assigned_date,
    fact_edqi.admission_form_bed_request_dt as admission_form_bed_request_date,
    fact_edqi.earliest_md_eval_dt as earliest_md_eval_date,
    fact_edqi.earliest_rn_report_dt as earliest_rn_report_date,
    fact_edqi.ready_to_plan_dt as ready_to_plan_date,
    stg_encounter.hospital_discharge_date
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    inner join {{ source('cdw_analytics', 'fact_edqi') }} as fact_edqi
        on fact_edqi.visit_key = cohort.visit_key
    inner join {{ ref('stg_encounter') }} as stg_encounter
      on cohort.visit_key = stg_encounter.visit_key
