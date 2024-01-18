select
    pcoti_episodes.episode_key,
    stg_pcoti_mpews.score_calc_datetime,
    stg_pcoti_mpews.pat_key,
    stg_pcoti_mpews.ip_clinical_deterioration_respiratory_rate,
    stg_pcoti_mpews.ip_clinical_deterioration_oxygen_saturation,
    stg_pcoti_mpews.ip_clinical_deterioration_capillary_refill,
    stg_pcoti_mpews.ip_clinical_deterioration_sbp,
    stg_pcoti_mpews.ip_clinical_deterioration_heart_rate,
    stg_pcoti_mpews.ip_clinical_deterioration_oxygen_requirement,
    stg_pcoti_mpews.ip_clinical_deterioration_respiratory_effort,
    stg_pcoti_mpews.mpews_total_score
from
    {{ref('stg_pcoti_mpews')}} as stg_pcoti_mpews
    inner join {{ref('pcoti_episodes')}} as pcoti_episodes
        on stg_pcoti_mpews.pat_key = pcoti_episodes.pat_key
        and stg_pcoti_mpews.score_calc_datetime >= pcoti_episodes.hospital_admit_date
        and stg_pcoti_mpews.score_calc_datetime <= pcoti_episodes.hospital_discharge_date
