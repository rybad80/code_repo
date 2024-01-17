{{ config(meta = {
    'critical': true
}) }}
/* stg_nursing_pp_p3_holiday
gets holiday name and observed date and joins to corresponding pay period
*/

with
get_holidays as (
    select
        date_key,
        holiday_name,
        case when holiday_union_only_ind = 1
            then case
                when lower(holiday_name_and_observed) like '%(observed)'
                then substring(holiday_name_and_observed, 1, length(holiday_name_and_observed) - 1)
                    || ', union only)'
                else holiday_name_and_observed || ' (union only)'
                end
            else holiday_name_and_observed
        end as holiday_name_and_observed,
        weekday_ind,
        holiday_all_employees_ind,
        holiday_union_only_ind
    from {{ ref('dim_date') }}
    where
        (holiday_all_employees_ind = 1
        or holiday_union_only_ind = 1)
        and weekday_ind = 1
),

pp_holiday_count as (
    select
        stg_nursing_pp_p2_pay_period.pp_start_dt_key,
        stg_nursing_pp_p2_pay_period.pp_end_dt_key,
        count(holiday_name_and_observed) as holiday_count,
        sum(holiday_all_employees_ind) as all_employee_holiday_count,
        sum(holiday_union_only_ind) as union_holiday_count
    from {{ ref('stg_nursing_pp_p2_pay_period') }} as stg_nursing_pp_p2_pay_period
    inner join get_holidays
        on get_holidays.date_key between stg_nursing_pp_p2_pay_period.pp_start_dt_key
            and stg_nursing_pp_p2_pay_period.pp_end_dt_key
    group by
        stg_nursing_pp_p2_pay_period.pp_start_dt_key,
        stg_nursing_pp_p2_pay_period.pp_end_dt_key
),

pp_holiday_rank as (
    select
        stg_nursing_pp_p2_pay_period.pp_start_dt_key,
        stg_nursing_pp_p2_pay_period.pp_end_dt_key,
        get_holidays.date_key,
        get_holidays.holiday_name_and_observed,
        pp_holiday_count.holiday_count,
        dense_rank() over (partition by stg_nursing_pp_p2_pay_period.pp_end_dt_key
            order by get_holidays.date_key) as holiday_rank_in_pp,
        pp_holiday_count.all_employee_holiday_count,
        pp_holiday_count.union_holiday_count,
        get_holidays.holiday_all_employees_ind,
        get_holidays.holiday_union_only_ind
    from {{ ref('stg_nursing_pp_p2_pay_period') }} as stg_nursing_pp_p2_pay_period
    inner join get_holidays
        on get_holidays.date_key between stg_nursing_pp_p2_pay_period.pp_start_dt_key
            and stg_nursing_pp_p2_pay_period.pp_end_dt_key
    left join pp_holiday_count
        on stg_nursing_pp_p2_pay_period.pp_end_dt_key = pp_holiday_count.pp_end_dt_key
),

pp_first_holiday as (
    select
        pp_start_dt_key,
        pp_end_dt_key,
        date_key as first_holiday_date_key,
        holiday_name_and_observed as first_holiday_name_and_observed,
        holiday_count,
        all_employee_holiday_count,
        union_holiday_count,
        holiday_all_employees_ind as first_holiday_all_employees_ind,
        holiday_union_only_ind as first_holiday_union_only_ind
    from pp_holiday_rank
    where holiday_rank_in_pp = 1
),

pp_second_holday as (
    select
        pp_start_dt_key,
        pp_end_dt_key,
        date_key as second_holiday_date_key,
        holiday_name_and_observed as second_holiday_name_and_observed,
        holiday_all_employees_ind as second_holiday_all_employees_ind,
        holiday_union_only_ind as second_holiday_union_only_ind
    from pp_holiday_rank
    where holiday_rank_in_pp = 2
)

select
    pp_first_holiday.pp_start_dt_key,
    pp_first_holiday.pp_end_dt_key,
    pp_first_holiday.holiday_count,
    pp_first_holiday.all_employee_holiday_count,
    pp_first_holiday.union_holiday_count,
    pp_first_holiday.first_holiday_date_key,
    pp_first_holiday.first_holiday_name_and_observed,
    pp_first_holiday.first_holiday_all_employees_ind,
    pp_first_holiday.first_holiday_union_only_ind,
    pp_second_holday.second_holiday_date_key,
    pp_second_holday.second_holiday_name_and_observed,
    pp_second_holday.second_holiday_all_employees_ind,
    pp_second_holday.second_holiday_union_only_ind,
    case pp_first_holiday.holiday_count
        when 1 then 'Holiday: ' || pp_first_holiday.first_holiday_name_and_observed
        when 2 then '2 Holidays: ' || pp_first_holiday.first_holiday_name_and_observed
            || ' and ' || pp_second_holday.second_holiday_name_and_observed
    end as pp_holiday_note
from pp_first_holiday
left join pp_second_holday
    on pp_first_holiday.pp_start_dt_key = pp_second_holday.pp_start_dt_key
    and pp_first_holiday.pp_end_dt_key = pp_second_holday.pp_end_dt_key
