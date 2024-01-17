{{ config(meta = {
    'critical': true
}) }}

/* stg_date_nursing_pay_period
to get efficiency for joins by date to get the nursing_pay_period record
*/
select
    full_date,
    nursing_pay_period.pp_end_dt_key,
    dim_date.date_key,
    nursing_pay_period.fiscal_year as pp_fiscal_year,
    master_date.f_mm as fiscal_month_int,
    nursing_pay_period.pay_period_number,
    dim_date.calendar_year as date_calendar_year,
    dim_date.fiscal_year as date_fiscal_year,
    dim_date.fiscal_quarter as date_fiscal_quarter,
    dim_date.month_int as date_month_int,
    dim_date.weekday_name,
    dim_date.weekday_ind,
    dim_date.business_day_ind,
    dim_date.holiday_all_employees_ind,
    dim_date.holiday_name_and_observed,
    dim_date.holiday_union_only_ind,
    nursing_pay_period.nursing_operations_window_ind,
    nursing_pay_period.nccs_platform_window_ind
from
    {{ ref('dim_date') }} as dim_date
    inner join {{ source('cdw', 'master_date') }} as master_date
        on dim_date.full_date = master_date.full_dt
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on dim_date.full_date
        between nursing_pay_period.pp_start_dt
        and nursing_pay_period.pp_end_dt
