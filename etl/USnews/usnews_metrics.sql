with stage as (
    select
        domain,
        subdomain,
        metric_name,
        metric_type,
        submission_year,
        metric_id,
        null as drill_down_one,
        null as drill_down_two,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', '0') -}} as denom_agg,
        {{- calculate_metric_value('num_agg', 'denom_agg') -}} as metric_value_submission_year
    from
        {{ ref('usnews_detail') }}
    group by
        domain,
        subdomain,
        metric_name,
        metric_type,
        submission_year,
        num_calculation,
        denom_calculation,
        drill_down_one,
        drill_down_two,
        metric_id
)

select 
    stage.domain,
    stage.subdomain,
    stage.metric_name,
    stage.metric_type,
    stage.submission_year,
    stage.metric_id,
    stage.metric_value_submission_year,
    target.top_threshold_value,
    target.middle_threshold_value,
    target.low_threshold_value,
    target.daily_top_threshold_value,
    target.daily_middle_threshold_value,
    target.daily_low_threshold_value,
    case
        when stage.metric_type = 'count' or stage.metric_type = 'sum'
            then sum(target.mtd_top_threshold_value)
        else target.top_threshold_value
    end as ytd_top_threshold_value,
    case
        when stage.metric_type = 'count' or stage.metric_type = 'sum'
            then sum(target.mtd_middle_threshold_value)
        else target.middle_threshold_value
    end as ytd_middle_threshold_value,
    case
        when stage.metric_type = 'count' or stage.metric_type = 'sum'
            then sum(target.mtd_low_threshold_value)
        else target.low_threshold_value
    end as ytd_low_threshold_value,
    case 
        when (direction = 'up'
                and (metric_value_submission_year >= ytd_top_threshold_value
                    or (ytd_top_threshold_value is null and metric_value_submission_year >= ytd_middle_threshold_value)
                    or (ytd_middle_threshold_value is null and metric_value_submission_year >= ytd_low_threshold_value)
                )
            )
            or (direction = 'down'
                    and (metric_value_submission_year <= ytd_top_threshold_value
                        or (ytd_top_threshold_value is null and metric_value_submission_year <= ytd_middle_threshold_value)
                        or (ytd_middle_threshold_value is null and metric_value_submission_year <= ytd_low_threshold_value)
                    )
                )
                then 'Achieving top threshold'
        
        when (direction = 'up' and metric_value_submission_year >= ytd_middle_threshold_value)
            or (direction = 'down' and  metric_value_submission_year <= ytd_middle_threshold_value)
                then 'Achieving middle threshold, but not top threshold'

        when (direction = 'up' and metric_value_submission_year >= ytd_low_threshold_value)
            or (direction = 'down' and  metric_value_submission_year <= ytd_low_threshold_value)
                then 'Achieving low threshold, but not middle threshold'

        when (direction = 'up'
                and (metric_value_submission_year < ytd_low_threshold_value
                    or (ytd_low_threshold_value is null and metric_value_submission_year < ytd_middle_threshold_value)
                    or (ytd_middle_threshold_value is null and metric_value_submission_year < ytd_top_threshold_value)
                )
            )
            or (direction = 'down'
                    and (metric_value_submission_year > ytd_top_threshold_value
                        or (ytd_low_threshold_value is null and metric_value_submission_year > ytd_middle_threshold_value)
                        or (ytd_middle_threshold_value is null and metric_value_submission_year > ytd_top_threshold_value)
                    )
                )
                then 'Not achieving low threshold'
        
        else 'No threshold provided'
    end as interpretation_status,
    target.direction as desired_direction,
    target.max_points, 
    target.max_weight

from
    stage as stage
        left join {{ ref('stg_usnews_targets') }} as target
            on stage.submission_year = target.submission_year
            and stage.metric_id = target.metric_id
            and date_trunc('month', current_date) >= date_trunc('month', target.visual_month) -- join on the visual months to get year to date target values
group by
    stage.domain,
    stage.subdomain,
    stage.metric_name,
    stage.metric_type,
    stage.submission_year,
    stage.metric_id,
    stage.metric_value_submission_year,
    target.top_threshold_value,
    target.middle_threshold_value,
    target.low_threshold_value,
    target.daily_top_threshold_value,
    target.daily_middle_threshold_value,
    target.daily_low_threshold_value,
    target.direction,
    target.max_points, 
    target.max_weight
order by
    stage.submission_year desc,
    stage.metric_id