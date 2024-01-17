with sti_encounters_ind as (
    select
        stg_care_network_sti_test.visit_key,
        stg_care_network_sti_test.pat_key,
        coalesce(stg_care_network_sti_test.ahq_given_ind, 0) as ahq_given_ind,
    /*If AHQ was available but not given, ahq_given_ind should be 0 rather than null*/
        stg_care_network_sti_test.ahq_sexually_active_ind,
        max(stg_care_network_sti_test.refusal_sti_visit_ind) as refusal_sti_visit_ind,
        max(stg_care_network_sti_test.eligible_sti_visit_pc_ind) as eligible_sti_visit_pc_ind,
        max(stg_care_network_sti_test.eligible_sti_visit_eop_ind) as eligible_sti_visit_eop_ind,
        max(stg_care_network_sti_test.chlamydia_test_visit_ind) as chlamydia_test_visit_ind,
        max(stg_care_network_sti_test.chlamydia_positive_visit_ind) as chlamydia_positive_visit_ind,
        max(stg_care_network_sti_test.chlamydia_test_past_yr_ind) as chlamydia_test_past_yr_ind,
        max(stg_care_network_sti_test.chlamydia_positive_past_yr_ind) as chlamydia_positive_past_yr_ind,
        max(stg_care_network_sti_test.chlamydia_last_test_date) as chlamydia_last_test_date
    from {{ ref('stg_care_network_sti_test') }} as stg_care_network_sti_test
    group by
        stg_care_network_sti_test.visit_key,
        stg_care_network_sti_test.pat_key,
        stg_care_network_sti_test.ahq_given_ind,
        stg_care_network_sti_test.ahq_sexually_active_ind
)

select
    sti_encounters_ind.visit_key,
    stg_encounter_outpatient.csn,
    sti_encounters_ind.pat_key,
    stg_patient.mrn,
    stg_encounter_outpatient.encounter_date,
    stg_encounter_outpatient.age_years,
    stg_patient.patient_name,
    stg_patient.sex,
    stg_patient.race_ethnicity,
    stg_patient.preferred_language,
    stg_encounter_outpatient.payor_group,
    worker.provider_worker_id,
    worker.provider_last_name,
    worker.provider_first_name,
    worker.provider_middle_initial,
    stg_encounter_outpatient.provider_name,
    worker.provider_type,
    lookup_care_network_department_cost_center_sites.department_id,
    lookup_care_network_department_cost_center_sites.department_display_name as department_name,
    lookup_care_network_department_cost_center_sites.cost_center_id,
    substring(lookup_care_network_department_cost_center_sites.cost_center_description, 7, length(
        lookup_care_network_department_cost_center_sites.cost_center_description)
    ) as practice_name,
    cost_center_id || '-' || cost_center_site_id as encounter_location,
    lookup_care_network_department_cost_center_sites.site_display_name as site_name,
    sti_encounters_ind.ahq_sexually_active_ind,
    sti_encounters_ind.refusal_sti_visit_ind,
    sti_encounters_ind.eligible_sti_visit_pc_ind,
    sti_encounters_ind.eligible_sti_visit_eop_ind,
    sti_encounters_ind.chlamydia_test_visit_ind,
    sti_encounters_ind.chlamydia_positive_visit_ind,
    sti_encounters_ind.chlamydia_test_past_yr_ind,
    sti_encounters_ind.chlamydia_positive_past_yr_ind,
    sti_encounters_ind.chlamydia_last_test_date
from sti_encounters_ind
inner join {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
    on stg_encounter_outpatient.visit_key = sti_encounters_ind.visit_key
inner join {{ ref('stg_patient') }} as stg_patient
    on stg_patient.pat_key = sti_encounters_ind.pat_key
inner join {{ ref('lookup_care_network_department_cost_center_sites') }}
    as lookup_care_network_department_cost_center_sites
    on lookup_care_network_department_cost_center_sites.department_id = stg_encounter_outpatient.department_id
inner join {{ ref('stg_care_network_distinct_worker') }} as worker
    on worker.prov_key = stg_encounter_outpatient.prov_key
