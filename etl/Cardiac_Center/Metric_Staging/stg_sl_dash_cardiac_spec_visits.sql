select
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'cost_center_id',
            'cost_center_site_id',
            'specialty_care_visit_type'
        ])
    }} as primary_key,
    post_date,
    post_date_month,
    cost_center_id,
    cost_center_description,
    cost_center_site_id,
    cost_center_site_name,
    specialty_care_visit_type,
    specialty_care_visit_actual,
    case when lower(specialty_care_visit_type) = 'est outpatient physician visits'
        then 'Established Patients'
        when lower(specialty_care_visit_type) = 'new outpatient physician visits'
        then 'New Patients'
        end as drill_down,
    'cardiac_scc_phys_visits' as metric_id
from
    {{ref('finance_sc_visit_daily_cost_center')}}
where
    lower(cost_center_description) = 'cardiology'
    and lower(specialty_care_visit_type) in ('new outpatient physician visits','est outpatient physician visits')
