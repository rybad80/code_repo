with pivoted as (
    select
        visit_key,
        {{ dbt_utils.pivot(
      'COHORT',
      dbt_utils.get_column_values(
        table=ref('ed_encounter_cohort_long'),
        column='COHORT',
        default='default_column_value'),
      suffix = "_IND" 
        ) }}
    from
        {{ref('ed_encounter_cohort_long')}}
    group by
        visit_key
)

select *
from pivoted
