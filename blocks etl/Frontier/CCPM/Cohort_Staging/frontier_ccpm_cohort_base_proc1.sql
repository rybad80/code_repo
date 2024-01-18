with
all_pats_n_init_dates as (--region
    select
        frontier_ccpm_clinical_trial.mrn,
        min(frontier_ccpm_clinical_trial.cy21_initial_date) as initial_date
    from {{ ref('frontier_ccpm_clinical_trial') }} as frontier_ccpm_clinical_trial
    group by frontier_ccpm_clinical_trial.mrn
    union
    select
        frontier_ccpm_med_grouper.mrn,
        frontier_ccpm_med_grouper.mg_initial_date as initial_date
        from {{ ref('frontier_ccpm_med_grouper') }} as frontier_ccpm_med_grouper
    union
    select
        frontier_ccpm_relapse.mrn,
        frontier_ccpm_relapse.min_rr_date as initial_date
        from {{ ref('frontier_ccpm_relapse') }} as frontier_ccpm_relapse
    --end region
)
select
    all_pats_n_init_dates.mrn,
    all_pats_n_init_dates.initial_date,
    frontier_ccpm_clinical_trial.clinical_trial_ind,
    frontier_ccpm_med_grouper.targeted_therapy_ind,
    frontier_ccpm_relapse.relapse_refractory_ind,
    row_number() over(
        partition by all_pats_n_init_dates.mrn
        order by initial_date)
    as obs_count
from all_pats_n_init_dates
    left join {{ ref('frontier_ccpm_clinical_trial') }} as frontier_ccpm_clinical_trial
        on all_pats_n_init_dates.mrn = frontier_ccpm_clinical_trial.mrn
            and all_pats_n_init_dates.initial_date = frontier_ccpm_clinical_trial.cy21_initial_date
    left join {{ ref('frontier_ccpm_med_grouper') }} as frontier_ccpm_med_grouper
        on all_pats_n_init_dates.mrn = frontier_ccpm_med_grouper.mrn
            and all_pats_n_init_dates.initial_date = frontier_ccpm_med_grouper.mg_initial_date
    left join {{ ref('frontier_ccpm_relapse') }} as frontier_ccpm_relapse
        on all_pats_n_init_dates.mrn = frontier_ccpm_relapse.mrn
            and all_pats_n_init_dates.initial_date = frontier_ccpm_relapse.min_rr_date
group by
    all_pats_n_init_dates.mrn,
    all_pats_n_init_dates.initial_date,
    frontier_ccpm_clinical_trial.clinical_trial_ind,
    frontier_ccpm_med_grouper.targeted_therapy_ind,
    frontier_ccpm_relapse.relapse_refractory_ind
