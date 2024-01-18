with stage as (
    select
        metric_name,
        drill_down_one,
        drill_down_two,
        domain,
        subdomain,
        metric_type,
        desired_direction, 
        {{ standard_date_fields(alias='stg_scorecard_data_calendar_by_metric') }},
        full_dt,
        f_day,
        c_day,
        as_of_date,
        metric_id
    from 
        {{ref('stg_scorecard_data_calendar_by_metric')}} as stg_scorecard_data_calendar_by_metric
    group by
        metric_name,
        drill_down_one,
        drill_down_two,
        domain,
        subdomain,
        metric_type,
        desired_direction, 
        {{ standard_date_fields(alias='stg_scorecard_data_calendar_by_metric') }},
        full_dt,
        f_day,
        c_day,
        as_of_date,
        metric_id
    union all
    select
        metric_name,
        'overall' as drill_down_one,
        'overall' as drill_down_two,
        domain,
        subdomain,
        metric_type,
        desired_direction, 
        {{ standard_date_fields(alias='stg_scorecard_data_calendar_by_metric') }},
        full_dt,
        f_day,
        c_day,
        as_of_date,
        metric_id
    from 
        {{ref('stg_scorecard_data_calendar_by_metric')}} as stg_scorecard_data_calendar_by_metric
    group by
        metric_name,
        drill_down_one,
        drill_down_two,
        domain,
        subdomain,
        metric_type,
        desired_direction, 
        {{ standard_date_fields(alias='stg_scorecard_data_calendar_by_metric') }},
        full_dt,
        f_day,
        c_day,
        as_of_date,
        metric_id
    union all
    select
        metric_name,
        drill_down_one,
        'drill_down_one' as drill_down_two,
        domain,
        subdomain,
        metric_type,
        desired_direction, 
        {{ standard_date_fields(alias='stg_scorecard_data_calendar_by_metric') }},
        full_dt,
        f_day,
        c_day,
        as_of_date,
        metric_id
    from 
        {{ref('stg_scorecard_data_calendar_by_metric')}} as stg_scorecard_data_calendar_by_metric
    group by
        metric_name,
        drill_down_one,
        drill_down_two,
        domain,
        subdomain,
        metric_type,
        desired_direction, 
        {{ standard_date_fields(alias='stg_scorecard_data_calendar_by_metric') }},
        full_dt,
        f_day,
        c_day,
        as_of_date,
        metric_id
)
select
    metric_name,
    drill_down_one,
    drill_down_two,
    domain,
    subdomain,
    metric_type,
    desired_direction, 
    {{ standard_date_fields(alias='stage') }},
    full_dt,
    f_day,
    c_day,
    as_of_date,
    metric_id
from
    stage
