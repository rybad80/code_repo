with
dev_hem_onc_cte as (--region
    select
        frontier_ccpm_med_grouper_proc1.mrn,
        1 as hem_onc_ind
    from {{ ref('frontier_ccpm_med_grouper_proc1') }} as frontier_ccpm_med_grouper_proc1
        inner join {{ ref('encounter_specialty_care') }} as encounter_specialty_care
            on frontier_ccpm_med_grouper_proc1.mrn = encounter_specialty_care.mrn
    where
        year(add_months(encounter_date, 6)) >= '2023'
        and (lower(specialty_name) like '%hem%'
            or lower(specialty_name) like '%onc%')
    group by
        frontier_ccpm_med_grouper_proc1.mrn
    --end region
),
dev_cvap_cte as (--region
    select distinct
        frontier_cva_encounter_cohort.mrn,
        1 as cvap_patient_ind
    from {{ ref('frontier_cva_encounter_cohort') }} as frontier_cva_encounter_cohort
        inner join {{ ref('frontier_ccpm_med_grouper_proc1') }} as frontier_ccpm_med_grouper_proc1
            on frontier_cva_encounter_cohort.mrn = frontier_ccpm_med_grouper_proc1.mrn
    --end region
),
join_patients as (--region
    select
        frontier_ccpm_med_grouper_proc1.mrn,
        medication_start_date,
        medication_end_date,
        frontier_ccpm_med_grouper_proc1.erx_onco_inv_ind,
        frontier_ccpm_med_grouper_proc1.erx_onco_enz_inhibitors_ind,
        frontier_ccpm_med_grouper_proc1.erx_onco_exclusion_meds_ind,
        hem_onc_ind,
        cvap_patient_ind,
        relapse_refractory_ind
    from {{ ref('frontier_ccpm_med_grouper_proc1') }} as frontier_ccpm_med_grouper_proc1
        left join dev_hem_onc_cte
            on frontier_ccpm_med_grouper_proc1.mrn = dev_hem_onc_cte.mrn
        left join dev_cvap_cte
            on frontier_ccpm_med_grouper_proc1.mrn = dev_cvap_cte.mrn
        left join {{ ref('frontier_ccpm_relapse') }} as rr_coh
            on frontier_ccpm_med_grouper_proc1.mrn = rr_coh.mrn
    group by
        frontier_ccpm_med_grouper_proc1.mrn,
        medication_start_date,
        medication_end_date,
        frontier_ccpm_med_grouper_proc1.erx_onco_inv_ind,
        frontier_ccpm_med_grouper_proc1.erx_onco_enz_inhibitors_ind,
        frontier_ccpm_med_grouper_proc1.erx_onco_exclusion_meds_ind,
        hem_onc_ind,
        cvap_patient_ind,
        relapse_refractory_ind
    --end region
),
first_pass_logic as (--region
    select
        mrn,
        medication_start_date,
        medication_end_date
    from
        join_patients
    where
        medication_start_date is not null
        and (erx_onco_inv_ind = 1
            or (erx_onco_enz_inhibitors_ind = 1
                and erx_onco_exclusion_meds_ind = 0))
        and (relapse_refractory_ind = 1
            or hem_onc_ind = 1 and cvap_patient_ind is null)
    --end region
)
select
    first_pass_logic.mrn,
    min(first_pass_logic.medication_start_date) as mg_initial_date,
    max(case
        when first_pass_logic.medication_end_date is null and stg_patient.deceased_ind = 1
        then stg_patient.death_date
        when first_pass_logic.medication_end_date is null and stg_patient.deceased_ind = 0
        then current_date
        else first_pass_logic.medication_end_date end)
    as mg_latest_date,
    1 as targeted_therapy_ind
from first_pass_logic
    left join {{ ref('stg_patient') }} as stg_patient
        on first_pass_logic.mrn = stg_patient.mrn
group by
    first_pass_logic.mrn
having year(add_months(mg_latest_date, 6)) >= '2023'
