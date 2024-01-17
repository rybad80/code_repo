select
    flowrw.fs_key,
    flowsheet_template.disp_nm as template_name,
    flowsheet_template.fs_temp_id as template_id,
    flowsheet.disp_nm as group_name,
    flowsheet.fs_id as group_id,
    flowrw.disp_nm as flowsheet_name,
    flowrw.fs_id  as flowsheet_id
from
    {{source('cdw', 'flowsheet')}} as flowsheet
    inner join {{source('cdw', 'flowsheet_group')}} as flowsheet_group
        on flowsheet.fs_key = flowsheet_group.fs_key
    inner join {{source('cdw', 'flowsheet')}} as flowrw
        on flowsheet_group.grp_fs_key = flowrw.fs_key
    inner join {{source('cdw', 'flowsheet_template_group')}} as flowsheet_template_group
        on flowsheet.fs_key = flowsheet_template_group.fs_key
    inner join {{source('cdw', 'flowsheet_template')}} as flowsheet_template
        on flowsheet_template_group.fs_temp_key = flowsheet_template.fs_temp_key
