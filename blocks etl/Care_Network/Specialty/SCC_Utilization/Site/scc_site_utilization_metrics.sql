with stg_site_utilization_metrics as (
    select
        scc_site_utilization.location_name,
        scc_site_utilization.site,
        scc_site_utilization.fiscal_quarter,
        scc_site_utilization.fiscal_year,
        scc_site_utilization.month_int,
        scc_site_utilization.session_type,
        stg_scc_site_exam_room_count.exam_room_count as total_rooms,
		(sum(reserved_rooms) / count(distinct resdate)) / count(distinct scc_site_utilization.session_type)
            as avg_rooms_reserved,
         (sum(room_used_ind) / count(distinct resdate)) / count(distinct scc_site_utilization.session_type)
            as avg_rooms_used,
        avg_rooms_reserved - avg_rooms_used as avg_rooms_rnu,
        case when sum(reserved_rooms) - sum(room_used_ind) != 0
            then round(sum(telehealth_check_ind) / (sum(reserved_rooms) - sum(room_used_ind)) * 100, 2)
            else null end as telehealth_percent_of_rnu,
        round((avg_rooms_reserved / total_rooms) * 100, 2) as reserved_percent,
        round((avg_rooms_used / total_rooms) * 100, 2) as utilized_percent,
        round((avg_rooms_rnu / avg_rooms_reserved) * 100, 2) as rnu_percent,
        round((avg_rooms_rnu / total_rooms) * 100, 2) as not_used_by_site_percent
    from {{ ref('scc_site_utilization') }} as scc_site_utilization
    inner join {{ ref('stg_scc_site_exam_room_count') }} as stg_scc_site_exam_room_count
        on stg_scc_site_exam_room_count.location_name = scc_site_utilization.location_name
    where scc_site_utilization.session_type is not null
    group by
        scc_site_utilization.location_name,
        scc_site_utilization.site,
        scc_site_utilization.fiscal_quarter,
        scc_site_utilization.fiscal_year,
        scc_site_utilization.month_int,
        stg_scc_site_exam_room_count.exam_room_count,
        scc_site_utilization.session_type
)

select
    location_name,
    site,
    fiscal_quarter,
    fiscal_year,
    fiscal_year || '-' || fiscal_quarter as fy_yy_qtr,
    case
        when month_int > 6 then month_int - 6
        else month_int + 6
    end as fy_month_int,
    fiscal_year || '-' || fy_month_int as fy_yy_month,
    session_type,
    total_rooms,
    avg_rooms_reserved as reserved_rooms,
    avg_rooms_used as used_rooms,
    avg_rooms_rnu as reserved_not_used,
    reserved_percent,
    utilized_percent,
    rnu_percent,
    not_used_by_site_percent
from stg_site_utilization_metrics
order by site, fy_yy_qtr, fy_yy_month, rnu_percent desc
