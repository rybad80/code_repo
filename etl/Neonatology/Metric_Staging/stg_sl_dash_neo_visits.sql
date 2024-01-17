/* get metrics on the visit, instead of the episode, level */
select distinct
    stg_sl_dash_neo_episodes.visit_key,
    stg_sl_dash_neo_episodes.pat_key,
    stg_sl_dash_neo_episodes.hospital_admit_date,
    stg_sl_dash_neo_episodes.hospital_discharge_date,
    stg_sl_dash_neo_episodes.hospital_discharge_day,
    stg_sl_dash_neo_episodes.hospital_los_days,
    stg_sl_dash_neo_episodes.cohort_group,
    stg_sl_dash_neo_episodes.admission_source,
    stg_sl_dash_neo_episodes.hospital_discharged_ind,
    /* distinguish this as a readmission to the hospital, not necessarily to the NICU */
    coalesce(encounter_readmission.readmit_30_day_ind, 0) as readmit_hospital_30_day_ind
from
    {{ ref('stg_sl_dash_neo_episodes') }} as stg_sl_dash_neo_episodes
    left join {{ ref('encounter_readmission') }} as encounter_readmission
        on encounter_readmission.index_visit_key = stg_sl_dash_neo_episodes.visit_key
