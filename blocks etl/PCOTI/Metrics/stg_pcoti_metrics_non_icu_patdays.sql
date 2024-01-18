select
    stg_pcoti_patdays.post_month as event_year_month,
    stg_pcoti_patdays.campus_name,
    stg_pcoti_patdays.department_group_name,
    sum(stg_pcoti_patdays.patdays) as denominator_patdays
from
    {{ ref('stg_pcoti_patdays') }} as stg_pcoti_patdays
where
    stg_pcoti_patdays.icu_ind = 0
group by
    stg_pcoti_patdays.post_month,
    stg_pcoti_patdays.campus_name,
    stg_pcoti_patdays.department_group_name
