with
initial_date_find as (
    select
        mrn,
        initial_date,
        case
            when clinical_trial_ind = '1' then 'ccpm clinical trial'
            when targeted_therapy_ind = '1' then 'ccpm targeted therapy'
            when relapse_refractory_ind = '1' then 'relapse refractory'
            else 'check' end
        as initial_date_source
    from {{ ref('frontier_ccpm_cohort_base_proc1') }}
    where
        obs_count = '1'
),
pat_ccpm_groups as (
    select
        mrn,
        max(case when clinical_trial_ind = 1 then 1 else 0 end) as clinical_trial_ind,
        max(case when targeted_therapy_ind = 1 then 1 else 0 end) as targeted_therapy_ind,
        max(case when relapse_refractory_ind = 1 then 1 else 0 end) as relapse_refractory_ind
    from {{ ref('frontier_ccpm_cohort_base_proc1') }}
    group by mrn
),
ccpm_group_find as (
    select
        mrn,
        case
            when clinical_trial_ind = '1' then 'ccpm clinical trial group'
            when targeted_therapy_ind = '1' then 'ccpm targeted therapy group'
            when relapse_refractory_ind = '1' then 'potential ccpm group'
            else 'check' end
        as patient_sub_cohort
    from pat_ccpm_groups
),
build_ccpm_patient_table as (
    select
        ccpm_group_find.mrn,
        ccpm_group_find.patient_sub_cohort,
        initial_date_find.initial_date,
        initial_date_find.initial_date_source
    from ccpm_group_find
        left join initial_date_find
            on ccpm_group_find.mrn = initial_date_find.mrn
)
select
    build_ccpm_patient_table.mrn,
    build_ccpm_patient_table.patient_sub_cohort,
    build_ccpm_patient_table.initial_date,
    build_ccpm_patient_table.initial_date_source,
    min(frontier_ccpm_clinical_trial.cy21_initial_date) as ct_initial_date,
    frontier_ccpm_med_grouper.mg_initial_date,
    min(frontier_ccpm_relapse.min_rr_date) as rr_initial_date,
    case
        when patient_sub_cohort = 'ccpm clinical trial group' then ct_initial_date
        when patient_sub_cohort = 'ccpm targeted therapy group' then mg_initial_date
        when patient_sub_cohort = 'potential ccpm group' then rr_initial_date
        else null end
    as sub_cohort_initial_date
from build_ccpm_patient_table
    left join {{ ref('frontier_ccpm_clinical_trial') }} as frontier_ccpm_clinical_trial
        on build_ccpm_patient_table.mrn = frontier_ccpm_clinical_trial.mrn
    left join {{ ref('frontier_ccpm_med_grouper') }} as frontier_ccpm_med_grouper
        on build_ccpm_patient_table.mrn = frontier_ccpm_med_grouper.mrn
    left join {{ ref('frontier_ccpm_relapse') }} as frontier_ccpm_relapse
        on build_ccpm_patient_table.mrn = frontier_ccpm_relapse.mrn
group by
    build_ccpm_patient_table.mrn,
    build_ccpm_patient_table.patient_sub_cohort,
    build_ccpm_patient_table.initial_date,
    build_ccpm_patient_table.initial_date_source,
    frontier_ccpm_med_grouper.mg_initial_date
