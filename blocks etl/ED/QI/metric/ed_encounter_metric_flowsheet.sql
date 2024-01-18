{%-
  set text_events = dbt_utils.get_column_values(
              table=ref('stg_ed_encounter_metric_flowsheet'),
              column='event_name',
              where="output_type = 'text'",
              default=[]
            )
-%}

{%-
  set numeric_events = dbt_utils.get_column_values(
              table=ref('stg_ed_encounter_metric_flowsheet'),
              column='event_name',
              where="output_type = 'numeric'",
              default=[]
            )
-%}

{%-
  set all_events = dbt_utils.get_column_values(
          table=ref('stg_ed_encounter_metric_flowsheet'),
          column='event_name',
          default=[]
        )
-%}

with pivoted as (
    select
      cohort.visit_key
    {%- if text_events %}
      ,{{ dbt_utils.pivot(
        'event_name',
        text_events,
        agg = "max",
        then_value = 'reporting_value_text',
        else_value = 'NULL',
        quote_identifiers=False
      )}}
    {%- endif %}
    {%- if numeric_events %}
      ,{{ dbt_utils.pivot(
        'event_name',
        numeric_events,
        agg = "max",
        then_value = 'reporting_value_numeric',
        else_value = 'NULL',
        quote_identifiers=False
      )}}
    {%- endif %}
    {%- if all_events %}
      ,{{ dbt_utils.pivot(
        'event_name',
        all_events,
        agg = "max",
        then_value = 'recorded_date',
        else_value = 'NULL',
        suffix = "_recorded_date",
        quote_identifiers=False
      )}}
    {%- endif %}
    from
      {{ref('stg_ed_encounter_cohort_all')}} as cohort
      left join {{ref('stg_ed_encounter_metric_flowsheet')}} as stg_ed_encounter_metric_flowsheet
        on cohort.visit_key = stg_ed_encounter_metric_flowsheet.visit_key
    group by
        cohort.visit_key
)

select *
from pivoted
