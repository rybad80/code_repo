select
    {{ dbt_utils.surrogate_key(
        [
            'dim_redcap_project.redcap_project_key',
            'stg_redcap_record_value.event_id',
            'stg_redcap_record_value.record_id',
            'stg_redcap_record_value.record_instance_num',
            'stg_redcap_record_value.field_name',
            'stg_redcap_record_value.record_value'
        ]
    )}} as redcap_record_value_key,
    coalesce(
        dim_redcap_project.redcap_project_key, 
        -1
    ) as redcap_project_key,
    stg_redcap_record_value.project_id,
    coalesce(
        dim_redcap_event.redcap_event_key, 
        -1
    ) as redcap_event_key,
    stg_redcap_record_value.event_id,
    stg_redcap_record_value.record_id,
    stg_redcap_record_value.record_instance_num,
    stg_redcap_record_value.field_name,
    stg_redcap_record_value.record_value
from
    {{ ref("stg_redcap_record_value") }} as stg_redcap_record_value
    left join {{ ref('dim_redcap_project') }} as dim_redcap_project
        on stg_redcap_record_value.project_id = dim_redcap_project.project_id
    left join {{ ref('dim_redcap_event') }} as dim_redcap_event
        on stg_redcap_record_value.project_id = dim_redcap_project.project_id
        and stg_redcap_record_value.event_id = dim_redcap_event.event_id
