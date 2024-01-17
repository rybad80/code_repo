with year_stage as (
    select
        f_yyyy as submission_year
    from
        {{source('cdw', 'master_date')}}
    where
        f_yyyy > 2019
        and f_yyyy <= year(current_date) + 1
),

metadata_stage as (
    select
        year_stage.submission_year,
        last_day(
            to_date(year_stage.submission_year - coalesce(metadata.lookback_years, 1)
                || lpad(metadata.month_end, 2, '0') || '01', 'yyyymmdd')
        ) as end_date,
        date_trunc('month', (end_date - metadata.duration_days + 1)) as start_date,
        metadata.*
    from
        year_stage as year_stage
    cross join
        {{ref('lookup_usnews_metadata')}} as metadata
    where
        (year_stage.submission_year >= metadata.submission_start_year
          and year_stage.submission_year <= metadata.submission_end_year)
        or (year_stage.submission_year >= metadata.submission_start_year
          and metadata.submission_end_year is null)
)

select distinct
    metadata_stage.submission_year,
    metadata_stage.start_date,
    metadata_stage.end_date,
    metadata_stage.duration_days,
    metadata_stage.division,
    metadata_stage.metric_name,
    metadata_stage.question_number,
    metadata_stage.age_gte,
    metadata_stage.age_lt,
    metadata_stage.sex,
    metadata_stage.billing_service,
    metadata_stage.metric_id,
    metadata_stage.num_calculation,
    metadata_stage.denom_calculation,
    metadata_stage.metric_type,
    metadata_stage.direction,
    metadata_stage.top_threshold_value,
    metadata_stage.middle_threshold_value,
    metadata_stage.low_threshold_value,
    metadata_stage.max_points,
    metadata_stage.max_weight,
    coalesce(usnwr_code_list.code_type, 'no code provided') as code_type,
    coalesce(usnwr_code_list.code, 'no code provided') as code,
    usnwr_code_list.inclusion_ind,
    usnwr_code_list.exclusion_ind
from
    metadata_stage
left join
    {{ref('usnews_code_list')}} as usnwr_code_list
        on  lower(usnwr_code_list.question_number) = lower(metadata_stage.question_number)
        and ((metadata_stage.submission_year >= usnwr_code_list.submission_start_year
            and metadata_stage.submission_year <= usnwr_code_list.submission_end_year)
        or (metadata_stage.submission_year >= usnwr_code_list.submission_start_year
            and usnwr_code_list.submission_end_year is null))
