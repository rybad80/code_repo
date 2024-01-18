select
    date_key,
    full_date,
    calendar_year,
    fiscal_year,
    fiscal_quarter,
    month_int,
    day_of_week,
    weekday_name,
    weekday_ind,
    case
        when weekday_ind = 1 and holiday_all_employees_ind = 0 then 1
        else 0
        end as business_day_ind,
    holiday_name,
    holiday_name_and_observed,
    holiday_all_employees_ind,
    holiday_union_only_ind
from
    {{ ref('stg_dim_date') }}
