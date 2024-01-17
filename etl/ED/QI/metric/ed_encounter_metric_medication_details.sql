with pivot as (
    select
      cohort.visit_key,
        {{ dbt_utils.pivot(
      'event_name',
      dbt_utils.get_column_values(
        table=ref('stg_ed_encounter_metric_medication_details'),
        column='event_name',
        default='default_column_value'),
      agg = "max",
      then_value = 'MEAS_VAL',
      else_value = 'NULL',
      quote_identifiers=False
        )}}
    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        left join {{ref('stg_ed_encounter_metric_medication_details')}} as stg_ed_encounter_metric_medication_route
          on stg_ed_encounter_metric_medication_route.visit_key = cohort.visit_key
    group by
        cohort.visit_key
)

select *
from
  pivot
