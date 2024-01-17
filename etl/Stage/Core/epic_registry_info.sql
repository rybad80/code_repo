{{ config(meta = {
    'critical': true
}) }}

select
    registry_config.registry_id as epic_registry_id,
    registry_config.registry_name,
    registry_config.display_name,
    registry_config.membership_rule_id,
    registry_config.registry_ini,
    -- dimensions
    zc_registry_type.internal_id as registry_type_id,
    zc_registry_type.name as registry_type,
    zc_reg_granularity.internal_id as granularity_id,
    zc_reg_granularity.name as granularity,
    zc_registry_category.internal_id as registry_category_id,
    zc_registry_category.name as registry_category,
    -- keys
    registry_config.base_registry_id
    -- registry_config_key
from
    {{ source('clarity_ods', 'registry_config') }} as registry_config
    left join {{ source('clarity_ods', 'zc_reg_granularity') }} as zc_reg_granularity using (reg_granularity_c)
    left join {{ source('clarity_ods', 'zc_registry_category') }} as zc_registry_category using (registry_cat_c)
    left join {{ source('clarity_ods', 'zc_registry_type') }} as zc_registry_type using (registry_type_c)
