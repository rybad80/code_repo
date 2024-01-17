with week_of_month as (
    select
        case
            when day(full_dt) between 1 and 7 then 'WEEK 1'
            when day(full_dt) between 8 and 14 then 'WEEK 2'
            when day(full_dt) between 15 and 21 then 'WEEK 3'
            when day(full_dt) between 22 and 28 then 'WEEK 4'
            when day(full_dt) > 28 then 'WEEK 5'
        end as week_number,
        case
            when
                day(
                    full_dt
                ) between 1 and 7 then year(
                    full_dt
                ) || '-' || month(full_dt) || '-'  || '1' || ' ' || '-' || ' ' || 'WEEK 1'
            when
                day(
                    full_dt
                ) between 8 and 14 then year(
                    full_dt
                ) || '-' || month(full_dt) || '-'  || '8' || ' ' || '-' || ' ' || 'WEEK 2'
            when
                day(
                    full_dt
                ) between 15 and 21 then year(
                    full_dt
                ) || '-' || month(full_dt) || '-'  || '15' || ' ' || '-' || ' ' || 'WEEK 3'
            when
                day(
                    full_dt
                ) between 22 and 28 then year(
                    full_dt
                ) || '-' || month(full_dt) || '-'  || '22' || ' ' || '-' || ' ' || 'WEEK 4'
            when
                day(
                    full_dt
                ) > 28 then year(full_dt) || '-' || month(full_dt) || '-'  || '29' || ' ' || '-' || ' ' || 'WEEK 5'
        end as week_start,
        full_dt as full_date,
        trim(day_nm) as day_name,
        month_nm as month_name
    from
        {{ source('cdw', 'master_date') }}
    where
        month(full_dt) >= 1
        and year(full_dt) >= 2022
)

select
	week_number,
	week_start,
	full_date,
	day_name,
	month_name
from
	week_of_month
