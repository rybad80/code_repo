select
    asp_ip_cap_cohort.visit_key,
    {{ dbt_utils.pivot(
      'treatment_group',
      dbt_utils.get_column_values(
        table=ref('stg_asp_ip_cap_metric_treatment'), 
        column='treatment_group',
        default='default_column_value'),
      agg = "max",
      then_value = 'treatment_type',
      else_value = 'NULL',
      quote_identifiers=False
    )}},
    --dbt-ify treamtent rows into columns
    max(case when stg_asp_ip_cap_metric_treatment.visit_key is not null
        then 1 else 0 end) as treatment_adherence_ind,
    --dbt-ify duration rows into columns
    {{ dbt_utils.pivot(
      'duration_group',
      dbt_utils.get_column_values(
       table=ref('stg_asp_ip_cap_metric_abx_duration'),
       column='duration_group',
       default='default_column_value'),
      agg = "max",
      then_value = 'duration_type',
      else_value = 'NULL',
      quote_identifiers=False
    )}},
    max(case when stg_asp_ip_cap_metric_abx_duration.visit_key is not null
        then 1 else 0 end) as duration_adherence_ind
from
    {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    left join {{ref('stg_asp_ip_cap_metric_treatment')}} as stg_asp_ip_cap_metric_treatment
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_metric_treatment.visit_key
    left join {{ref('stg_asp_ip_cap_metric_abx_duration')}} as stg_asp_ip_cap_metric_abx_duration
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_metric_abx_duration.visit_key
group by
    asp_ip_cap_cohort.visit_key
