select
    visit_key,
    action_date,
    group_concat(initcap(ntmx_grouper), ';') as ntmx_med_names,
    count(*) as ntmx_med_count
from
    {{ ref('stg_naki_meds') }}
group by
    visit_key,
    action_date
