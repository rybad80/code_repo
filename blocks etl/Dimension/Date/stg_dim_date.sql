-- https://at.chop.edu/hr/Pages/Holiday-Schedule.aspx

select
    dt_key as date_key,
    full_dt as full_date,
    c_mm as month_int,
    c_yyyy as calendar_year,
    f_yyyy as fiscal_year,
    f_qtr as fiscal_quarter,
    day_of_wk as day_of_week,
    initcap(substr(day_nm, 1, 3)) as weekday_name,
    weekday_ind,
    case
        when substr(dt_key, 5, 8) in ('0101', '0704', '0828', '1225') then 1
        when substr(dt_key, 5, 8) in ('0619') and f_yyyy >= 2023 then 1
        else 0
        end as by_date_ind,
    case when substr(dt_key, 5, 8) in ('1224', '1231') then 1 else 0 end as eve_date_ind,
    case
        -- mlk jr day: 3rd monday in jan
        when c_mm =  1 and day(full_dt) between 15 and 21 and day_of_wk = 2 then 1
        -- memorial day: last monday in may
        when c_mm =  5 and day(full_dt) between 25 and 31 and day_of_wk = 2 then 1
        -- labor day: 1st monday in sept
        when c_mm =  9 and day(full_dt) between  1 and  7 and day_of_wk = 2 then 1
        -- thanksgiving: 4th thursday in nov
        when c_mm = 11 and day(full_dt) between 22 and 28 and day_of_wk = 5 then 1
        else 0
        end as by_nth,
    case
        when substr(dt_key, 5, 8) = '0101' then 'New Years Day'
        when substr(dt_key, 5, 8) in ('0619') and f_yyyy >= 2023 then 'Juneteenth'
        when substr(dt_key, 5, 8) = '0704' then 'Fourth of July'
        when substr(dt_key, 5, 8) = '0828' then 'Norman Rayford Day'
        when substr(dt_key, 5, 8) = '1224' then 'Christmas Eve'
        when substr(dt_key, 5, 8) = '1225' then 'Christmas Day'
        when substr(dt_key, 5, 8) = '1231' then 'New Years Eve'
        when c_mm =  1 and day(full_dt) between 15 and 21 and day_of_wk = 2 then 'MLK Jr Day'
        when c_mm =  5 and day(full_dt) between 25 and 31 and day_of_wk = 2 then 'Memorial Day'
        when c_mm =  9 and day(full_dt) between  1 and  7 and day_of_wk = 2 then 'Labor Day'
        when c_mm = 11 and day(full_dt) between 22 and 28 and day_of_wk = 5 then 'Thanksgiving Day'
        end as holiday_name,
     case
        when
            lead(
                by_date_ind
            ) over(
                partition by 1 order by full_dt
            ) = 1 and day_of_wk = 6 then lead(holiday_name) over(partition by 1 order by full_dt) || ' (observed)'
        when
            lag(
                by_date_ind
            ) over(
                partition by 1 order by full_dt
            ) = 1 and day_of_wk = 2 then  lag(holiday_name) over(partition by 1 order by full_dt) || ' (observed)'
        else holiday_name
        end as holiday_name_and_observed,
    case
        when c_mm = 8 and day(full_dt) between 27 and 29 then 0 -- union only, can be monday or friday
        when lead(by_date_ind) over(partition by 1 order by full_dt) = 1 and day_of_wk = 6 then 1
        when lag(by_date_ind) over(partition by 1 order by full_dt) = 1 and day_of_wk = 2 then 1
        when by_nth = 1 then 1
        when by_date_ind = 1 then 1
        else 0
        end as holiday_all_employees_ind,
    case
        when
            substr(dt_key, 5, 8) = '0828'
            or (-- observed
                c_mm = 8
                and (
                    (day(full_dt) = 27 and day_of_wk = 6)
                    or (day(full_dt) = 29 and day_of_wk = 2 )
                )
            )
            then 1
        else 0
        end as holiday_union_only_ind
from
    {{ source('cdw', 'master_date') }}
