select
    usnews_metadata_calendar.submission_year,
    date_trunc('month', master_date.full_dt) as visual_month,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    usnews_metadata_calendar.metric_type,
    usnews_metadata_calendar.duration_days,
    usnews_metadata_calendar.direction,
    usnews_metadata_calendar.max_points,
    usnews_metadata_calendar.max_weight,

    /* top threshold values */
    usnews_metadata_calendar.top_threshold_value,
    case
        when usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum'
            then usnews_metadata_calendar.top_threshold_value / usnews_metadata_calendar.duration_days
        when usnews_metadata_calendar.metric_type = 'percentage' or usnews_metadata_calendar.metric_type = 'rate'
            then usnews_metadata_calendar.top_threshold_value
        else null
    end as daily_top_threshold_value,
    case
        when (usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum')
        and date_trunc('month', current_date) = date_trunc('month', visual_month)
            then daily_top_threshold_value * (day(current_date) - 1)
        when (usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum')
        and date_trunc('month', current_date) > date_trunc('month', visual_month)
            then daily_top_threshold_value * day(last_day(date_trunc('month', master_date.full_dt)))
        else daily_top_threshold_value
    end as mtd_top_threshold_value,

    /* middle threshold values */
    usnews_metadata_calendar.middle_threshold_value,
    case
        when usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum'
            then usnews_metadata_calendar.middle_threshold_value / usnews_metadata_calendar.duration_days
        when usnews_metadata_calendar.metric_type = 'percentage' or usnews_metadata_calendar.metric_type = 'rate'
            then usnews_metadata_calendar.middle_threshold_value
        else null
    end as daily_middle_threshold_value,
    case
        when (usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum')
        and date_trunc('month', current_date) = date_trunc('month', visual_month)
            then daily_middle_threshold_value * (day(current_date) - 1)
        when (usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum')
        and date_trunc('month', current_date) > date_trunc('month', visual_month)
            then daily_middle_threshold_value * day(last_day(date_trunc('month', master_date.full_dt)))
        else daily_middle_threshold_value
    end as mtd_middle_threshold_value,

    /* low threshold values */
    usnews_metadata_calendar.low_threshold_value,
    case
        when usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum'
            then usnews_metadata_calendar.low_threshold_value / usnews_metadata_calendar.duration_days
        when usnews_metadata_calendar.metric_type = 'percentage' or usnews_metadata_calendar.metric_type = 'rate'
            then usnews_metadata_calendar.low_threshold_value
        else null
    end as daily_low_threshold_value,
    case
        when (usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum')
        and date_trunc('month', current_date) = date_trunc('month', visual_month)
            then daily_low_threshold_value * (day(current_date) - 1)
        when (usnews_metadata_calendar.metric_type = 'count' or usnews_metadata_calendar.metric_type = 'sum')
        and date_trunc('month', current_date) > date_trunc('month', visual_month)
            then daily_low_threshold_value * day(last_day(date_trunc('month', master_date.full_dt)))
        else daily_low_threshold_value
    end as mtd_low_threshold_value

from
   {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
    inner join
   {{source('cdw', 'master_date')}} as master_date
    on master_date.full_dt between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
group by
    usnews_metadata_calendar.submission_year,
    date_trunc('month', master_date.full_dt),
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    usnews_metadata_calendar.metric_type,
    usnews_metadata_calendar.duration_days,
    usnews_metadata_calendar.direction,
    usnews_metadata_calendar.max_points,
    usnews_metadata_calendar.max_weight,
    usnews_metadata_calendar.top_threshold_value,
    usnews_metadata_calendar.middle_threshold_value,
    usnews_metadata_calendar.low_threshold_value
