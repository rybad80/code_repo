{#
Dyanmically generating a cte for each varchar length used within pipeline,
in order to achieve flexability of variable lengths, be good stewards of 
data storage, and avoid the Netezza spring field limiation.
#}

{%-
  set varchar_lengths = dbt_utils.get_column_values(
    table=ref('stg_ed_encounter_metric_procedure_details'),
    column='event_varchar_length',
    where="event_varchar_length is not null",
    default='0'
  )
-%}

{%-
  set all_events = dbt_utils.get_column_values(
    table=ref('stg_ed_encounter_metric_procedure_details'),
    column='event_name',
    default='default_column_value'
  )
-%}


with pivot_timestamps as (
    select
      stg_ed_encounter_metric_procedure_details.visit_key,
      {{ dbt_utils.pivot(
        'event_name',
        all_events,
        agg = "min",
        then_value = 'EVENT_TIMESTAMP',
        else_value = 'NULL',
        suffix = "_date",
        quote_identifiers=False
      ) }}
    from
        {{ref('stg_ed_encounter_metric_procedure_details')}} as stg_ed_encounter_metric_procedure_details
    group by
        stg_ed_encounter_metric_procedure_details.visit_key
)

{% for len in varchar_lengths %}
,

pivot_varchar_{{ len }} as (
    select
      stg_ed_encounter_metric_procedure_details.visit_key,
        {{ dbt_utils.pivot(
          'event_name',
          dbt_utils.get_column_values(
            table=ref('stg_ed_encounter_metric_procedure_details'),
            column='event_name',
            where="event_varchar_length = " ~ len,
            default='default_column_value'
          ),
          agg = "max",
          then_value = "
            case
                when length(MEAS_VAL) > EVENT_VARCHAR_LENGTH
                  then '[TRUNCATED] '
                else ''
            end ||
            cast(MEAS_VAL as varchar(" ~ len ~ ")) ||
            case
                when length(MEAS_VAL) > EVENT_VARCHAR_LENGTH
                  then '...'
                else ''
            end  
          ",
          else_value = 'NULL',
          quote_identifiers=False
        )}}
    from
        {{ref('stg_ed_encounter_metric_procedure_details')}} as stg_ed_encounter_metric_procedure_details
    group by
        stg_ed_encounter_metric_procedure_details.visit_key
)
{% endfor %}

select
  cohort.visit_key
{% for e in all_events %}
  ,{{ e }}
  ,{{ e }}_date
{% endfor %}
from
  {{ref('stg_ed_encounter_cohort_all')}} as cohort
  left join pivot_timestamps as pivot_timestamps
    on cohort.visit_key = pivot_timestamps.visit_key
{% for len in varchar_lengths %}
  left join pivot_varchar_{{ len }} as pivot_varchar_{{ len }}
    on cohort.visit_key = pivot_varchar_{{ len }}.visit_key
{% endfor %}
