select
    stg_usage_qlikview_sessions.qlikview_app_user_key,
    stg_usage_qlikview_sessions.application_title,
    stg_usage_qlikview_sessions.user_name,
    sum(
        case
        when stg_usage_qlikview_sessions.usage_date >= stg_usage_qlikview_sessions.upd_dt - 1 then 1
        else 0
        end
    ) as usage_1_day,
    sum(
        case
        when stg_usage_qlikview_sessions.usage_date >= stg_usage_qlikview_sessions.upd_dt - 7 then 1
        else 0
        end
    ) as usage_7_day,
    sum(
        case
        when stg_usage_qlikview_sessions.usage_date >= stg_usage_qlikview_sessions.upd_dt - 90 then 1
        else 0
        end
    ) as usage_90_day,
    sum(
        case
        when stg_usage_qlikview_sessions.usage_date >= stg_usage_qlikview_sessions.upd_dt - 180 then 1
        else 0
        end
    ) as usage_180_day,
    sum(
        case
        when stg_usage_qlikview_sessions.usage_date >= stg_usage_qlikview_sessions.upd_dt - 365 then 1
        else 0
        end
    ) as usage_365_day,
    min(stg_usage_qlikview_sessions.usage_date) as first_usage_date,
    max(stg_usage_qlikview_sessions.usage_date) as last_usage_date,
    case when worker.ad_login is not null then 1 else 0 end as worker_ind,
    stg_usage_qlikview_sessions.qlikview_app_key,
    stg_usage_qlikview_sessions.asset_inventory_key,
    stg_usage_qlikview_sessions.upd_dt,
    'manual' as upd_by
from
    {{ ref('stg_usage_qlikview_sessions') }} as stg_usage_qlikview_sessions
    left join {{ ref('worker') }} as worker
        on stg_usage_qlikview_sessions.user_name = worker.ad_login
group by
    stg_usage_qlikview_sessions.qlikview_app_user_key,
    stg_usage_qlikview_sessions.qlikview_app_key,
    stg_usage_qlikview_sessions.user_name,
    stg_usage_qlikview_sessions.application_title,
    worker.ad_login,
    stg_usage_qlikview_sessions.asset_inventory_key,
    stg_usage_qlikview_sessions.upd_dt
