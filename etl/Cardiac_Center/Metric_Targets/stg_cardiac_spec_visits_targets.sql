select
    post_date_month,
    'count' as metric_type,
    case when lower(specialty_care_visit_type) = 'est outpatient physician visits'
        then 'Established Patients'
        when lower(specialty_care_visit_type) = 'new outpatient physician visits'
        then 'New Patients'
        end as drill_down,
    sum(specialty_care_visit_budget) as specialty_care_visit_budget,
    'cardiac_scc_phys_visits' as metric_id
from
    {{ ref('finance_sc_visit_month_cost_center')}}
where
    lower(cost_center_description) = 'cardiology'
    and lower(specialty_care_visit_type) in ('new outpatient physician visits', 'est outpatient physician visits')
group by
    post_date_month,
    specialty_care_visit_type
