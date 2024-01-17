select
    mrn,
    patient_name,
    pat_key,
    min(index_date) as index_date,
    exc_cohort,
    date_cutoff_ind
from {{ref('stg_usnews_pulm_asthma_exclusion_lung')}}
group by
    mrn,
    patient_name,
    pat_key,
    exc_cohort,
    date_cutoff_ind
union all
select
    mrn,
    patient_name,
    pat_key,
    min(index_date) as index_date,
    exc_cohort,
    date_cutoff_ind
from {{ref('stg_usnews_pulm_asthma_exclusion_vent')}}
group by
    mrn,
    patient_name,
    pat_key,
    exc_cohort,
    date_cutoff_ind
union all
select
    mrn,
    patient_name,
    pat_key,
    min(encounter_date) as index_date,
    exc_cohort,
    date_cutoff_ind
from {{ref('stg_usnews_pulm_asthma_exclusion_historic_dx')}}
group by
    mrn,
    patient_name,
    pat_key,
    exc_cohort,
    date_cutoff_ind
union all
select
    mrn,
    patient_name,
    pat_key,
    min(index_date) as index_date,
    exc_cohort,
    date_cutoff_ind
from {{ref('stg_usnews_pulm_asthma_exclusion_other')}}
group by
    mrn,
    patient_name,
    pat_key,
    exc_cohort,
    date_cutoff_ind
