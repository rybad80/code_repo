select distinct
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'cost_center_id',
            'cost_center_site_id',
            'primary_diagnosis_code'
        ])
    }} as primary_key,
    finance_sc_visit_actual.post_date,
	finance_sc_visit_actual.post_date_month,
    finance_sc_visit_actual.cost_center_id,
    finance_sc_visit_actual.cost_center_description,
    finance_sc_visit_actual.cost_center_site_id,
    finance_sc_visit_actual.cost_center_site_name,
    finance_sc_visit_actual.primary_diagnosis_code,
    finance_sc_visit_actual.primary_diagnosis_name,
    coalesce(lookup_neuro_dx_grouping.dx_grouping, 'Other') as dx_grouping,
    finance_sc_visit_actual.specialty_care_visit_actual
from
    {{ref('finance_sc_visit_actual')}} as finance_sc_visit_actual
left join {{ref('lookup_neuro_dx_grouping')}} as lookup_neuro_dx_grouping
    on finance_sc_visit_actual.primary_diagnosis_code like lookup_neuro_dx_grouping.dx_cd
where
    lower(finance_sc_visit_actual.cost_center_description) in ('neurology', 'neurosurgery')
    and lower(finance_sc_visit_actual.statistic_name) = 'new outpatient physician visits'
    and revenue_statistic_ind = 1
