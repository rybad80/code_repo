{#
`expected_columns` is needed because this model dynamically creates columns based on 
data in upstream tables. However, if expected data is not there for some reason, 
e.g. CI filtering by date, then downstream can fail because the expected column does not exist.
This adds placeholders for expected columns if they do not exist.
#}

{% set expected_columns = dbt_utils.get_column_values(table=ref('lookup_ed_events_smart_data_element_all'), column='event_name') %}

{%-
  set text_events = dbt_utils.get_column_values(
              table=ref('stg_ed_encounter_metric_smart_data_element'),
              column='event_name',
              where="output_type = 'text'",
              default=[]
            )
-%}

{%-
  set numeric_events = dbt_utils.get_column_values(
              table=ref('stg_ed_encounter_metric_smart_data_element'),
              column='event_name',
              where="output_type = 'numeric'",
              default=[]
            )
-%}

{%-
  set timestamp_events = dbt_utils.get_column_values(
              table=ref('stg_ed_encounter_metric_smart_data_element'),
              column='event_name',
              where="output_type = 'timestamp'",
              default=[]
            )
-%}

{%-
  set all_events = dbt_utils.get_column_values(
          table=ref('stg_ed_encounter_metric_smart_data_element'),
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
    {%- if timestamp_events %}
      ,{{ dbt_utils.pivot(
        'event_name',
        timestamp_events,
        agg = "max",
        then_value = 'reporting_value_timestamp',
        else_value = 'NULL',
        quote_identifiers=False
      )}}
    {%- endif %}
    {%- if all_events %}
      ,{{ dbt_utils.pivot(
        'event_name',
        all_events,
        agg = "max",
        then_value = 'entered_date',
        else_value = 'NULL',
        suffix = "_date",
        quote_identifiers=False
      )}}
    {%- endif %}
    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        left join
            {{ref('stg_ed_encounter_metric_smart_data_element')}} as stg_ed_encounter_metric_smart_data_element
          on stg_ed_encounter_metric_smart_data_element.visit_key = cohort.visit_key
    group by
        cohort.visit_key
)
select
  *
  {%- for column in expected_columns -%}
    {%- set column = column.lower() -%}
    {%- set all_events = all_events | map('lower') | list -%}
    {% if column not in all_events %}
      ,null as {{ column }}
    {% endif -%}
  {%- endfor -%}
from pivoted
