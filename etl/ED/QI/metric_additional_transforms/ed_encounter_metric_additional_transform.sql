{%-
  set transform_tables = [
    'stg_ed_encounter_metric_transform_febrile_infant',
    'stg_ed_encounter_metric_transform_asthma_pro',
    'stg_ed_encounter_metric_transform_urinalysis',
    'stg_ed_encounter_metric_transform_bh_firearms'
  ]
-%}

{%- set transform_relations = [] -%}
{%- for tbl in transform_tables -%}
  {%- do transform_relations.append(ref(tbl)) -%}
{%- endfor -%}

select
  cohort.visit_key,
  {%- for tbl in transform_tables %}
    {{ dbt_utils.star(from=ref(tbl), except=["visit_key"]) }}{% if not loop.last %}, {% endif -%}
  {% endfor %}
from
  {{ref('stg_ed_encounter_cohort_all')}} as cohort
  {%- for rel in transform_relations %}
    left join {{rel}}
      on cohort.visit_key = {{rel}}.visit_key
  {%- endfor %}
