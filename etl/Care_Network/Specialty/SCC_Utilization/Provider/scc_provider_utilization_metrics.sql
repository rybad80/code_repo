with stg_provider_utilization_metrics as (
    select
        scc_provider_utilization.provider_id,
        scc_provider_utilization.site,
        scc_provider_utilization.specialty,
        scc_provider_utilization.fiscal_year || '-' || scc_provider_utilization.fiscal_quarter as fy_yy_qtr,
        (
            sum(scc_provider_utilization.reserved_rooms) / count(distinct scc_provider_utilization.resdate)
        ) / count(distinct scc_provider_utilization.session_type) as avg_reserved_rooms,
        (
            sum(scc_provider_utilization.room_used) / count(distinct scc_provider_utilization.resdate)
        ) / count(distinct scc_provider_utilization.session_type) as avg_rooms_used,
        avg_reserved_rooms - avg_rooms_used as avg_room_rnu,
        case
            when sum(scc_provider_utilization.reserved_rooms) - sum(scc_provider_utilization.room_used) != 0
            then round(
                sum(
                    scc_provider_utilization.telehealth_check_ind
                ) / (sum(scc_provider_utilization.reserved_rooms) - sum(scc_provider_utilization.room_used)) * 100,
                2
            )
            else null end as telehealth_percent_of_rnu,
        round(
            sum(scc_provider_utilization.room_used) / sum(scc_provider_utilization.reserved_rooms) * 100, 2
        ) as utilization_percent,
        round(
            (
                sum(scc_provider_utilization.reserved_rooms) - sum(scc_provider_utilization.room_used)
            ) / sum(scc_provider_utilization.reserved_rooms) * 100,
            2
        ) as rnu_percent
    from {{ref('scc_provider_utilization')}} as scc_provider_utilization
    group by
        scc_provider_utilization.provider_id,
        scc_provider_utilization.site,
        scc_provider_utilization.specialty,
        fy_yy_qtr
)

select * from stg_provider_utilization_metrics
