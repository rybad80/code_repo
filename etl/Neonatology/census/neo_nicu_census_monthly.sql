select
    census_month,
    sum(initial_nicu_admissions) as initial_nicu_admissions,
    sum(all_nicu_admissions) as all_nicu_admissions,
    sum(nicu_patient_days) as nicu_patient_days,
    sum(nicu_vent_days) as nicu_vent_days,
    round(avg(blue_census), 1) as blue_census,
    round(avg(green_census), 1) as green_census,
    round(avg(orange_census), 1) as orange_census,
    round(avg(purple_census), 1) as purple_census,
    round(avg(red_census), 1) as red_census,
    round(avg(yellow_census), 1) as yellow_census,
    round(avg(missing_team_census), 1) as missing_team_census,
    round(avg(total_nicu_census), 1) as total_nicu_census,
    sum(itcu_neo_admissions) as itcu_neo_admissions,
    sum(itcu_neo_patient_days) as itcu_neo_patient_days,
    round(avg(itcu_neo_census), 1) as itcu_neo_census,
    sum(sdu_births) as sdu_births,
    sum(sdu_to_nicu) as sdu_to_nicu,
    sum(sdu_to_cicu) as sdu_to_cicu,
    sum(sdu_only) as sdu_only,
    sum(sdu_to_other) as sdu_to_other
from
    {{ ref('neo_nicu_census_daily') }}
group by
    census_month
