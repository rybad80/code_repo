with fs_watcher_activation as (
    select
        flowsheet_all.pat_key,
        flowsheet_all.visit_key,
        flowsheet_all.fs_rec_key,
        flowsheet_all.seq_num,
        flowsheet_all.flowsheet_id,
        flowsheet_all.flowsheet_record_id,
        flowsheet_all.flowsheet_name,
        flowsheet_all.recorded_date
    from
        {{ ref('flowsheet_all') }} as flowsheet_all
    where
        flowsheet_all.flowsheet_id = 15802
)

select
    fs_watcher_activation.pat_key,
    fs_watcher_activation.visit_key,
    'Watcher Activation' as event_type_name,
    'WATCHER' as event_type_abbrev,
    null as department_group_name,
    null as bed_care_group,
    fs_watcher_activation.recorded_date as event_start_date,
    null as event_end_date
from
    fs_watcher_activation
where
    fs_watcher_activation.recorded_date >= '2017-01-01'
