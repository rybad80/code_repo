
{% set refs = [
    ref('stg_ed_encounter_cohort_all'),
    ref('stg_ed_encounter_cohort_anaphylaxis'),
    ref('stg_ed_encounter_cohort_seen'),
    ref('stg_ed_encounter_cohort_fever_cvc'),
    ref('stg_ed_encounter_cohort_fever_discharge'),
    ref('stg_ed_encounter_cohort_scd_pain'),
    ref('stg_ed_encounter_cohort_scd_fever'),
    ref('stg_ed_encounter_cohort_asthma'),
    ref('stg_ed_encounter_cohort_cellulitis'),
    ref('stg_ed_encounter_cohort_rn_standing_orders'),
    ref('stg_ed_encounter_cohort_febrile_infant'),
    ref('stg_ed_encounter_cohort_urinalysis'),
    ref('stg_ed_encounter_cohort_hyperbili'),
    ref('stg_ed_encounter_cohort_bh_fast_track'),
    ref('stg_ed_encounter_cohort_intussusception'),
    ref('stg_ed_bh_encounter_cohort_firearms'),
    ref('stg_ed_encounter_cohort_high_occupancy_hold')
] %}

{{ dbt_utils.union_relations(
    relations = refs,
    include = ["PAT_KEY","VISIT_KEY", "COHORT", "SUBCOHORT"],
    column_override = {
        "COHORT": "varchar(100)", 
        "SUBCOHORT": "varchar(100)"},
    source_column_name="dbt_source_relation") }}
