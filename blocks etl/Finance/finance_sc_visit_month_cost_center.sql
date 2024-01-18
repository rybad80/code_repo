{{ config(meta = {
    'critical': true
}) }}

select
    finance_sc_visit_daily_cost_center.post_date_month,
    finance_sc_visit_daily_cost_center.company_id,
    finance_sc_visit_daily_cost_center.cost_center_id,
    finance_sc_visit_daily_cost_center.cost_center_description,
    finance_sc_visit_daily_cost_center.cost_center_site_id,
    finance_sc_visit_daily_cost_center.cost_center_site_name,
    finance_sc_visit_daily_cost_center.specialty_care_visit_type,
    sum(
        finance_sc_visit_daily_cost_center.specialty_care_visit_budget
    ) as specialty_care_visit_budget,
    sum(
        finance_sc_visit_daily_cost_center.specialty_care_visit_actual
    ) as specialty_care_visit_actual
from
    {{ref('finance_sc_visit_daily_cost_center')}}
        as finance_sc_visit_daily_cost_center
group by
    finance_sc_visit_daily_cost_center.post_date_month,
    finance_sc_visit_daily_cost_center.company_id,
    finance_sc_visit_daily_cost_center.cost_center_id,
    finance_sc_visit_daily_cost_center.cost_center_description,
    finance_sc_visit_daily_cost_center.cost_center_site_id,
    finance_sc_visit_daily_cost_center.cost_center_site_name,
    finance_sc_visit_daily_cost_center.specialty_care_visit_type
