with stg_division_utilization_metrics as (
    select
        scc_division_utilization.department_id,
        scc_division_utilization.department_name,
        scc_division_utilization.location_name,
        scc_division_utilization.site,
        scc_division_utilization.specialty,
        scc_division_utilization.fiscal_quarter,
        scc_division_utilization.fiscal_year,
        scc_division_utilization.month_int,
        scc_division_utilization.session_type,
        (
            sum(scc_division_utilization.reserved_rooms)
            / count(distinct scc_division_utilization.resdate)
        ) / count(distinct scc_division_utilization.session_type) as avg_rooms_reserved,
        (
            sum(scc_division_utilization.room_used_ind)
            / count(distinct scc_division_utilization.resdate)
        ) / count(distinct scc_division_utilization.session_type) as avg_rooms_used,
        avg_rooms_reserved - avg_rooms_used as avg_rooms_rnu,
        case
            when sum(scc_division_utilization.reserved_rooms)
            - sum(scc_division_utilization.room_used_ind) != 0
            then round(
                sum(
                    scc_division_utilization.telehealth_check_ind
                ) / (sum(scc_division_utilization.reserved_rooms)
                - sum(scc_division_utilization.room_used_ind)) * 100,
                2
            )
            else null end as telehealth_percent_of_rnu,
        round((avg_rooms_used / avg_rooms_reserved) * 100, 2) as utilized_percent,
        round((avg_rooms_rnu / avg_rooms_reserved) * 100, 2) as rnu_percent
    from {{ref('scc_division_utilization')}} as scc_division_utilization
    where scc_division_utilization.session_type is not null
    group by
        scc_division_utilization.department_id,
        scc_division_utilization.department_name,
        scc_division_utilization.location_name,
        scc_division_utilization.site,
        scc_division_utilization.specialty,
        scc_division_utilization.session_type,
        scc_division_utilization.fiscal_year,
        scc_division_utilization.month_int,
        scc_division_utilization.fiscal_quarter
)

select
    department_id,
    department_name,
    specialty,
    location_name,
    site,
    fiscal_year,
    fiscal_quarter,
    fiscal_year || '-' || fiscal_quarter as fy_yy_qtr,
    case
        when month_int > 6 then month_int - 6
        else month_int + 6
    end as fy_month_int,
    fiscal_year || '-' || fy_month_int as fy_yy_month,
    session_type,
    avg_rooms_reserved as reserved_rooms,
    avg_rooms_used as used_rooms,
    avg_rooms_rnu as reserved_not_used,
    utilized_percent,
    rnu_percent
from stg_division_utilization_metrics
