with flowsheet_template_limited as (
    select flowsheet_template.fs_temp_key
    from
        {{ source('cdw', 'flowsheet_template') }} as flowsheet_template

    where
        -- limit to template "CHOP IP VASCULAR ACCESS"
        flowsheet_template.fs_temp_id = 40001015
),

flowsheet_template_group_limited as (
    select
        flowsheet_template_group.fs_temp_key,
        flowsheet_template_group.fs_key
    from
        {{ source('cdw', 'flowsheet_template_group') }} as flowsheet_template_group

)

select
    flowsheet_template_limited.fs_temp_key,
    flowsheet_template_group_limited.fs_key
from
    flowsheet_template_limited
inner join flowsheet_template_group_limited
    on flowsheet_template_limited.fs_temp_key = flowsheet_template_group_limited.fs_temp_key
