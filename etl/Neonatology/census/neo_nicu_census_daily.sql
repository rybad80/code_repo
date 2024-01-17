select
    stg_neo_nicu_census_nicu.census_date,
    date(
        year(stg_neo_nicu_census_nicu.census_date)
        || '-'
        || month(stg_neo_nicu_census_nicu.census_date)
        || '-01'
    ) as census_month,
    stg_neo_nicu_census_nicu.initial_nicu_admissions,
    stg_neo_nicu_census_nicu.all_nicu_admissions,
    stg_neo_nicu_census_nicu.nicu_patient_days,
    stg_neo_nicu_census_nicu.nicu_vent_days,
    coalesce(stg_neo_nicu_census_nicu.blue_census, 0) as blue_census,
    coalesce(stg_neo_nicu_census_nicu.green_census, 0) as green_census,
    coalesce(stg_neo_nicu_census_nicu.orange_census, 0) as orange_census,
    coalesce(stg_neo_nicu_census_nicu.purple_census, 0) as purple_census,
    coalesce(stg_neo_nicu_census_nicu.red_census, 0) as red_census,
    coalesce(stg_neo_nicu_census_nicu.yellow_census, 0) as yellow_census,
    coalesce(stg_neo_nicu_census_nicu.missing_team_census, 0) as missing_team_census,
    coalesce(stg_neo_nicu_census_nicu.total_nicu_census, 0) as total_nicu_census,
    coalesce(stg_neo_nicu_census_itcu.itcu_neo_admissions, 0) as itcu_neo_admissions,
    coalesce(stg_neo_nicu_census_itcu.itcu_neo_patient_days, 0) as itcu_neo_patient_days,
    coalesce(stg_neo_nicu_census_itcu.itcu_neo_census, 0) as itcu_neo_census,
    coalesce(stg_neo_nicu_census_sdu.sdu_births, 0) as sdu_births,
    coalesce(stg_neo_nicu_census_sdu.sdu_to_nicu, 0) as sdu_to_nicu,
    coalesce(stg_neo_nicu_census_sdu.sdu_to_cicu, 0) as sdu_to_cicu,
    coalesce(stg_neo_nicu_census_sdu.sdu_only, 0) as sdu_only,
    coalesce(stg_neo_nicu_census_sdu.sdu_to_other, 0) as sdu_to_other
from
    {{ ref('stg_neo_nicu_census_nicu') }} as stg_neo_nicu_census_nicu
    left join {{ ref('stg_neo_nicu_census_itcu') }} as stg_neo_nicu_census_itcu
        on stg_neo_nicu_census_itcu.census_date = stg_neo_nicu_census_nicu.census_date
    left join {{ ref('stg_neo_nicu_census_sdu') }} as stg_neo_nicu_census_sdu
        on stg_neo_nicu_census_sdu.census_date = stg_neo_nicu_census_nicu.census_date
