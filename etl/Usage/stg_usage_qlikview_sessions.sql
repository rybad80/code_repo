
select
    {{
        dbt_utils.surrogate_key([
            'qlikview_usage.application_name',
            'qlikview_usage.date::date',
            "lower(regexp_extract(qlikview_usage.userid, '\w+$'))"
        ])
    }} as qlikview_app_usage_key,
    regexp_replace(qlikview_usage.application_name, '(?i)\.qvw$', '') as application_title,
    lower(regexp_extract(qlikview_usage.userid, '\w+$')) as user_name,
    nvl2(worker.ad_login, 1, 0) as worker_ind,
    qlikview_usage.date::date as usage_date,
    {{
        dbt_utils.surrogate_key([
            'qlikview_usage.application_name',
            "lower(regexp_extract(qlikview_usage.userid, '\w+$'))"
        ])
    }} as qlikview_app_user_key,
    {{ dbt_utils.surrogate_key(['qlikview_usage.application_name']) }} as qlikview_app_key,
    {{ dbt_utils.surrogate_key([ "'qlik view'", 'application_title']) }} as asset_inventory_key,
    max(date::date) over(partition by 1) as upd_dt,
    'manual' as upd_by
from
    {{ source('manual_ods', 'qlikview_usage') }} as qlikview_usage
    left join {{ref('worker')}}
        on worker.ad_login = lower(regexp_extract(qlikview_usage.userid, '\w+$'))
where
    qlikview_usage.userid is not null
    and lower(qlikview_usage.application_name) != '.qvw'
group by
    qlikview_usage.application_name,
    qlikview_usage.date::date,
    qlikview_usage.userid,
    worker.ad_login,
    qlikview_app_user_key,
    qlikview_app_key,
    asset_inventory_key
