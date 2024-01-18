{{ config(meta = {
    'critical': true
}) }}
/* stg_nursing_pp_p1_schedule_period
identify moving date windows for nursing strategy and operations purposes
*/
with get_current_date_attributes as (
    select
        stg_dim_date.full_date as date_today,
        stg_dim_date.fiscal_year as current_fiscal_year,
        master_date.f_mm,
        case
            when master_date.f_mm < 5
            then 'show full PRIOR FY'
            when master_date.f_mm between 5 and 9
            then 'show only CURRENT FY'
            else 'show current fy PLUS TWO QTRS'
            end as nursing_ops_window,
        stg_dim_date.fiscal_year - 1 as prior_fiscal_year,
        stg_dim_date.fiscal_year + 1 as next_fiscal_year,
        case
            when nursing_ops_window = 'show current fy PLUS TWO QTRS'
            then 6   /* reflects how many months to show of next fy */
            else 0
            end as next_year_peek
    from
        {{ ref('stg_dim_date') }} as stg_dim_date
        inner join {{ source('cdw', 'master_date') }} as master_date
        on stg_dim_date.full_date = full_dt
    where
        stg_dim_date.full_date = current_date
),

use_date_range as (
    select
        get_current_date_attributes.date_today,
        get_current_date_attributes.current_fiscal_year,
        get_current_date_attributes.nursing_ops_window,
        get_current_date_attributes.f_mm,
        get_current_date_attributes.prior_fiscal_year,
        get_current_date_attributes.next_fiscal_year,
        get_current_date_attributes.next_year_peek,
        case
            when nursing_ops_window = 'show full PRIOR FY'
            then add_months(curr_fy_end.full_dt + 1, -24)
            else add_months(curr_fy_end.full_dt + 1, -12)
            end as nursing_ops_start,
        case
            when nursing_ops_window = 'show current fy PLUS TWO QTRS'
            then master_date.full_dt
            else curr_fy_end.full_dt
            end as nursing_ops_end,
        coalesce(
            case
                when next_year_peek != 0
                then master_date.full_dt
                end,
            curr_fy_end.full_dt) as end_date_range
    from
    get_current_date_attributes
    left join {{ source('cdw', 'master_date') }} as master_date
        on get_current_date_attributes.next_fiscal_year = master_date.f_yyyy
        and master_date.f_mm = next_year_peek
        and master_date.last_day_month_ind = 1
    left join {{ source('cdw', 'master_date') }}  as curr_fy_end
        on get_current_date_attributes.current_fiscal_year = curr_fy_end.f_yyyy
        and curr_fy_end.f_mm = 12
        and curr_fy_end.last_day_month_ind = 1
),

year_in_6_buckets as (
    select
        -- base date chosen by nursing
        date('2022-07-23') as base_date,
        stg_dim_date.full_date,
        extract(epoch from date(stg_dim_date.full_date) - base_date ) as days_from_base_date,
        floor(days_from_base_date / 42.0) as six_week_bucket
    from
        {{ ref('stg_dim_date') }} as stg_dim_date
        inner join {{ source('cdw', 'master_pay_periods') }} as master_pay_periods
            on master_pay_periods.end_dt_key = stg_dim_date.date_key
    where
        stg_dim_date.fiscal_year >= 2019
),

nursing_schedule_period_end as (
    select
        six_week_bucket,
        min(full_date) as six_week_period_end_date
    from year_in_6_buckets
    group by six_week_bucket
)

select
    stg_dim_date_start.date_key as schedule_start_dt_key,
    stg_dim_date_end.date_key as schedule_end_dt_key,
    stg_dim_date_start.full_date as schedule_start_dt,
    nursing_schedule_period_end.six_week_period_end_date as schedule_end_dt,
    stg_dim_date_end.fiscal_year as schedule_fiscal_year,
    'FY' || chr(39)
        || substr(stg_dim_date_end.fiscal_year, 3, 2) as schedule_fiscal_year_label,
    to_char(schedule_start_dt, 'mm/dd/yy')
        || ' - '
        || to_char(schedule_end_dt, 'mm/dd/yy') as schedule_date_range,
    case
        when schedule_fiscal_year = prior_fiscal_year
        then 1 else 0
        end as  prior_fiscal_year_ind,
    case
        when schedule_fiscal_year = current_fiscal_year
        then 1 else 0
        end as  current_fiscal_year_ind,
    case
        when schedule_fiscal_year = next_fiscal_year
        then 1 else 0
        end as  next_fiscal_year_ind,
    case
        when schedule_end_dt > nursing_ops_start
        and schedule_start_dt < nursing_ops_end
        then 1 else 0
        end as nursing_operations_window_ind,
    case
        when schedule_fiscal_year > 2021
        then 1 else 0
        end as nccs_platform_window_ind
from
    nursing_schedule_period_end
    cross join use_date_range
    inner join  {{ ref('stg_dim_date') }}  as stg_dim_date_end
        on nursing_schedule_period_end.six_week_period_end_date = stg_dim_date_end.full_date
    inner join {{ ref('stg_dim_date') }}  as stg_dim_date_start
        on nursing_schedule_period_end.six_week_period_end_date - 41 = stg_dim_date_start.full_date
