select
    base_population.csn,
    base_population.mrn,
    base_population.contact_date,
    base_population.month_year,
    base_population.fiscal_year,
    base_population.department_name,
    base_population.specialty,
    base_population.department_center,
    base_population.revenue_location,
    base_population.specialty_care_ind,
    base_population.primary_care_ind,
    copay_collection_rate.copay_amount_due,
    copay_collection_rate.high_copay_amount_due_ind,
    copay_collection_rate.copay_amount_paid,
    copay_collection_rate.rsn_non_coll,
    encounters_verified.verified_ind,
    encounters_verified.encounter_to_be_verified_ind,
    number_of_checkins.check_in_count,
    referral_shells_created.referral_shell_ind,
    referral_shells_created.referral_req_ind,
    workflow_duration.emp_workflow_duration,
    workflows_without_warning.workflow_count,
    workflows_without_warning.workflow_no_warning_count
from
    {{ref('stg_patient_access_base_population')}} as base_population
    left join {{ref('stg_patient_access_copay_collection_rate')}} as copay_collection_rate
        on copay_collection_rate.pat_enc_csn_id = base_population.csn
    left join {{ref('stg_patient_access_encounters_verified')}} as encounters_verified
        on encounters_verified.pat_enc_csn_id = base_population.csn
    left join {{ref('stg_patient_access_number_of_check_ins')}} as number_of_checkins
        on number_of_checkins.pat_enc_csn_id = base_population.csn
    left join {{ref('stg_patient_access_referral_shells_created')}} as referral_shells_created
        on referral_shells_created.pat_enc_csn_id = base_population.csn
    left join {{ref('stg_patient_access_workflow_duration')}} as workflow_duration
        on workflow_duration.pat_enc_csn_id = base_population.csn
    left join {{ref('stg_patient_access_workflows_without_warning')}} as workflows_without_warning
        on workflows_without_warning.pat_enc_csn_id = base_population.csn
