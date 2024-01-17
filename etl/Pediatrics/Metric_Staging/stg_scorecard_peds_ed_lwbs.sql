select
    fact_edqi.visit_key as primary_key,
    date(fact_edqi.arrive_ed_dt) as metric_date,
    fact_edqi.initial_ed_department_center_abbr,
    fact_edqi.ed_patients_presenting_ind as denom,
    case
        when fact_edqi.ed_patients_presenting_ind = 1
            and fact_edqi.ed_patients_seen_ind = 0
            then 1 else 0
        end as pats_lwbs
from
    {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
where
    date(fact_edqi.arrive_ed_dt) >= '01/01/2019'
