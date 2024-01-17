select *
from {{ref('ed_events')}}
where
    event_category = 'procedure_order'
    and event_repeat_number = 1

union all

select
    cast({{ string_literal(ref('stg_ed_events_procedure_order')) }} as {{ type_string() }}) as dbt_source_relation,
    visit_key,
    event_category,
    event_name || '_count' as event_name,
    event_source,
    null as event_timestamp,
    cast(count(distinct proc_ord_root_key) as varchar(10)) as meas_val,
    1 as event_repeat_number
from
    {{ ref('stg_ed_events_procedure_order') }}
group by
    dbt_source_relation,
    visit_key,
    event_category,
    event_name,
    event_source
