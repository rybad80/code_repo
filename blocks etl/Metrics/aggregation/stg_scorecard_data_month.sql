with overall as (
    select
        domain,
        subdomain,
        metric_name,
        metric_type,
        {{ numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'day_of_mm') -}} as denom_agg,
        'overall' as drill_down_one,
        'overall' as drill_down_two,
        {{- standard_date_fields() -}},
        metric_id
    from
        {{ ref('stg_scorecard_data_overall') }}
    group by
        domain,
        subdomain,
        metric_name,
        metric_type,
        num_calculation,
        denom_calculation,
        {{- standard_date_fields() -}},
        metric_id
),

drill_down_one as (
    select
        domain,
        subdomain,
        metric_name,
        metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'day_of_mm') -}} as denom_agg,
        drill_down_one,
        'drill_down_one' as drill_down_two,
            {{- standard_date_fields() -}},
        metric_id
    from
        {{ ref('stg_scorecard_data_drill_down_one') }}
    group by
        domain,
        subdomain,
        metric_name,
        metric_type,
        num_calculation,
        denom_calculation,
        drill_down_one,
        {{- standard_date_fields() -}},
        metric_id
),

drill_down_two as (
    select
        domain,
        subdomain,
        metric_name,
        metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'day_of_mm') -}} as denom_agg,
        drill_down_one,
        drill_down_two,
        {{- standard_date_fields() -}},
        metric_id
    from
        {{ ref('stg_scorecard_data_drill_down_two') }}
    group by
        domain,
        subdomain,
        metric_name,
        metric_type,
        num_calculation,
        denom_calculation,
        drill_down_one,
        drill_down_two,
        {{- standard_date_fields() -}},
        metric_id
),

overall_drilldown_union as (
    select * from overall
    union all
    select * from drill_down_one
    union all
    select * from drill_down_two
)

select
    domain,
    subdomain,
    metric_name,
    metric_type,
    num_agg,
    denom_agg,
    drill_down_one,
    drill_down_two,
    {{- standard_date_fields() -}},
    metric_id,
    {{- calculate_metric_value('num_agg', 'denom_agg') -}} as metric_value_month_td
from
    overall_drilldown_union
group by
    domain,
    subdomain,
    metric_name,
    metric_type,
    num_agg,
    denom_agg,
    drill_down_one,
    drill_down_two,
    {{- standard_date_fields() -}},
    metric_id
