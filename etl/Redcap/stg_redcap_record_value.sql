select distinct
    coalesce(redcap_data.project_id, -1) as project_id,
    coalesce(redcap_data.event_id, -1) as event_id,
    redcap_data.record as record_id,
    coalesce(redcap_data.instance, 1) as record_instance_num,
    redcap_data.field_name,
    redcap_data.value::varchar(4000) as record_value
from
    {{ ref('stg_redcap_all')}} as redcap_data
