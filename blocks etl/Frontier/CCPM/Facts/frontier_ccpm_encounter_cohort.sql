with
ccpm_base as (
    select
        frontier_ccpm_cohort_base.mrn,
        frontier_ccpm_cohort_base.patient_sub_cohort,
        frontier_ccpm_cohort_base.initial_date,
        frontier_ccpm_cohort_base.initial_date_source,
        frontier_ccpm_cohort_base.ct_initial_date,
        frontier_ccpm_cohort_base.mg_initial_date,
        frontier_ccpm_cohort_base.rr_initial_date,
        frontier_ccpm_cohort_base.sub_cohort_initial_date--,
        --frontier_ccpm_relapse.rr_group_label,
        --frontier_ccpm_relapse.max_dx_count
    from
        {{ ref('frontier_ccpm_cohort_base') }} as frontier_ccpm_cohort_base
        left join {{ ref('frontier_ccpm_relapse') }} as frontier_ccpm_relapse
            on frontier_ccpm_cohort_base.mrn = frontier_ccpm_relapse.mrn
                and frontier_ccpm_cohort_base.rr_initial_date = frontier_ccpm_relapse.min_rr_date
    group by
        frontier_ccpm_cohort_base.mrn,
        frontier_ccpm_cohort_base.patient_sub_cohort,
        frontier_ccpm_cohort_base.initial_date,
        frontier_ccpm_cohort_base.initial_date_source,
        frontier_ccpm_cohort_base.ct_initial_date,
        frontier_ccpm_cohort_base.mg_initial_date,
        frontier_ccpm_cohort_base.rr_initial_date,
        frontier_ccpm_cohort_base.sub_cohort_initial_date
),
patient_dgd_hx as (
    select
        ccpm_base.mrn,
        min(service_date) as first_ccpm_dgd,
        max(service_date) as most_recent_ccpm_dgd,
        1 as dgd_test_ind
    from
        ccpm_base
        left join {{ ref('procedure_billing') }} as procedure_billing
            on ccpm_base.mrn = procedure_billing.mrn
            and procedure_billing.service_date >= ccpm_base.initial_date
    where
        lower(cpt_code) = '81455' --solid paired tumor panel- dgd
    group by
        ccpm_base.mrn
)
select
    stg_encounter.visit_key,
    ccpm_base.mrn,
    ccpm_base.patient_sub_cohort,
    ccpm_base.initial_date,
    ccpm_base.initial_date_source,
    ccpm_base.ct_initial_date,
    ccpm_base.mg_initial_date,
    frontier_ccpm_med_grouper.mg_latest_date,
    ccpm_base.rr_initial_date,
    ccpm_base.sub_cohort_initial_date,
    --ccpm_base.rr_group_label,
    --ccpm_base.max_dx_count,
    coalesce(patient_dgd_hx.dgd_test_ind, 0) as dgd_test_ind,
    patient_dgd_hx.first_ccpm_dgd,
    patient_dgd_hx.most_recent_ccpm_dgd,
    stg_encounter.csn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    stg_encounter.provider_name,
    stg_encounter.provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    case when stg_encounter_inpatient.visit_key is not null then 1 else 0 end as inpatient_ind,
    --stg_encounter.inpatient_ind,
    stg_encounter_inpatient.admission_department,
    stg_encounter.pat_key,
    stg_hsp_acct_xref.hsp_acct_key
from
    ccpm_base
    left join {{ ref('frontier_ccpm_med_grouper') }} as frontier_ccpm_med_grouper
        on ccpm_base.mrn = frontier_ccpm_med_grouper.mrn
    left join patient_dgd_hx
        on ccpm_base.mrn = patient_dgd_hx.mrn
    left join {{ ref('stg_encounter') }} as stg_encounter
        on ccpm_base.mrn = stg_encounter.mrn
        and date(stg_encounter.encounter_date)
            >= date(ccpm_base.initial_date)
    left join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
        on stg_encounter.visit_key = stg_encounter_inpatient.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
where
    year(add_months(stg_encounter.encounter_date, 6)) >= '2023'
    and stg_encounter.encounter_date < current_date
    and ((patient_sub_cohort = 'ccpm targeted therapy group'
            and stg_encounter.encounter_date <= frontier_ccpm_med_grouper.mg_latest_date)
        or patient_sub_cohort != 'ccpm targeted therapy group')
