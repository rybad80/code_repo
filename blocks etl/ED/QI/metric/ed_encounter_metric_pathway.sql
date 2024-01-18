with pivoted as (
    select
      cohort.visit_key,
        {{ dbt_utils.pivot(
      'event_name',
      dbt_utils.get_column_values(
        table=ref('stg_ed_encounter_metric_pathway'),
        column='event_name',
        default='default_column_value'),
      agg = "max",
      then_value = '1',
      else_value = '0',
      suffix = "_ind",
      quote_identifiers=False
        )}}
    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        left join {{ref('stg_ed_encounter_metric_pathway')}} as stg_ed_encounter_metric_pathway 
          on stg_ed_encounter_metric_pathway.visit_key = cohort.visit_key
    group by
        cohort.visit_key
)

select *
from pivoted