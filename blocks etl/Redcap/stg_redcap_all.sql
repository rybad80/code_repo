{{
    config(
        materialized = 'view',
        meta = {
            'critical': true
            }
        )
}}


select
    project_id,
    event_id,
    record,
    field_name,
    value,
    instance,
    upd_dt
from
    {{ source('ods_redcap_porter', 'redcap_data') }}
union all
select
    project_id,
    event_id,
    record,
    field_name,
    value,
    instance,
    upd_dt
from
    {{ source('ods_redcap_porter', 'redcap_data2') }}
