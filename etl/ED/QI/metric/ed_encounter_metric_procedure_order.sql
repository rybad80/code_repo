with pivoted as (
    select
      cohort.visit_key,
        {{ dbt_utils.pivot(
            'event_name',
            dbt_utils.get_column_values(
                table=ref('stg_ed_encounter_metric_procedure_order'),
                column='event_name',
                default=['default_column_value'],
                where="event_name not like '%\_count'"),
            agg = "max",
            then_value = 'MEAS_VAL',
            else_value = 'NULL',
            quote_identifiers=False
        )}},
        {{ dbt_utils.pivot(
            'event_name',
            dbt_utils.get_column_values(
                table=ref('stg_ed_encounter_metric_procedure_order'),
                column='event_name',
                default=['default_column_value'],
                where="event_name not like '%\_count'"),
            agg = "min",
            then_value = 'EVENT_TIMESTAMP',
            else_value = 'NULL',
            suffix = "_date",
            quote_identifiers=False
        )}},
        {{ dbt_utils.pivot(
            'event_name',
            dbt_utils.get_column_values(
                table=ref('stg_ed_encounter_metric_procedure_order'),
                column='event_name',
                default=['default_column_value'],
                where="event_name like '%\_count'"),
            agg = "max",
            then_value = 'MEAS_VAL',
            else_value = 'NULL',
            quote_identifiers=False
        )}}
    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        left join {{ref('stg_ed_encounter_metric_procedure_order')}} as stg_ed_encounter_metric_procedure_order 
          on stg_ed_encounter_metric_procedure_order.visit_key = cohort.visit_key
    group by
        cohort.visit_key
)

select *
from pivoted
