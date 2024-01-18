{{ config(meta = {
    'critical': true
}) }}

with visits_actual as (
    select
        fact_financial_statistic_op_spec_visit.post_dt_key,
        department.gl_prefix,
        department.rpt_grp_10,
        department.rpt_grp_3,
        fact_financial_statistic_op_spec_visit.loc_key,
        fact_financial_statistic_op_spec_visit.mstr_op_spec_visit_cat_key,
        fact_reimbursement.dx1_key,
        max(
            case when fact_reimbursement.chrg_svc_dt_key < 20151001 then 1 else 0 end
        ) as icd9_dx_charge_ind,
        sum(fact_financial_statistic_op_spec_visit.stat_measure) as specialty_care_visit_actual
    from
        {{source('cdw_analytics', 'fact_financial_statistic_op_spec_visit')}} as fact_financial_statistic_op_spec_visit --noqa: L016
        inner join {{source('cdw', 'fact_reimbursement')}} as fact_reimbursement
            on fact_reimbursement.chrg_tx_id = fact_financial_statistic_op_spec_visit.chrg_tx_id
        inner join {{source('cdw', 'department')}} as department
            on department.dept_key = fact_financial_statistic_op_spec_visit.dept_key
    where
        fact_financial_statistic_op_spec_visit.post_dt_key >= 20190701
        and fact_financial_statistic_op_spec_visit.mstr_op_spec_visit_cat_key in (34, 35, 36)
        and fact_reimbursement.det_type_key = 1
    group by
        fact_financial_statistic_op_spec_visit.post_dt_key,
        department.gl_prefix,
        department.rpt_grp_10,
        department.rpt_grp_3,
        fact_financial_statistic_op_spec_visit.loc_key,
        fact_financial_statistic_op_spec_visit.mstr_op_spec_visit_cat_key,
        fact_reimbursement.dx1_key
    having sum(fact_financial_statistic_op_spec_visit.stat_measure) != 0
)

select
    to_date(visits_actual.post_dt_key, 'yyyymmdd') as post_date,
    date_trunc('month', post_date) as post_date_month,
    loc.gl_prefix as company_id,
    visits_actual.gl_prefix as cost_center_id,
    cost_center.cost_cntr_nm as cost_center_description, --listed as cost_center_name in finance_charges_actual
    visits_actual.rpt_grp_3 as cost_center_site_id,
    workday_cost_center_site.cost_cntr_site_nm as cost_center_site_name,
    coalesce(visits_actual.rpt_grp_10, '0') as division,
    coalesce(case when visits_actual.icd9_dx_charge_ind = 1 then icd_9_dx.icd9_cd
        else icd_10_dx.icd10_cd end, '0') as primary_diagnosis_code,
    case when visits_actual.icd9_dx_charge_ind = 1 then icd_9_dx.dx_nm
        else icd_10_dx.dx_nm end as primary_diagnosis_name,
    master_statistic.stat_nm as statistic_name,
    case
        when lower(visits_actual.rpt_grp_10) = 'emergency medicine'
            or visits_actual.mstr_op_spec_visit_cat_key = 36
        then 0 else 1
    end as revenue_statistic_ind,
    visits_actual.specialty_care_visit_actual
from
    visits_actual
    left join {{source('cdw', 'diagnosis')}} as icd_10_dx
        on icd_10_dx.dx_key = visits_actual.dx1_key
            and icd_10_dx.icd10_ind = 1
            and icd_10_dx.seq_num = 1
    left join {{source('cdw', 'diagnosis')}} as icd_9_dx
        on icd_9_dx.dx_key = visits_actual.dx1_key
            and icd_9_dx.icd10_ind = 0
            and icd_9_dx.seq_num = 1
    left join {{source('cdw', 'cost_center')}} as cost_center
        on cost_center.cost_cntr_id = visits_actual.gl_prefix
            and cost_center.create_by = 'WORKDAY'
    left join {{source('workday', 'workday_cost_center_site')}} as workday_cost_center_site
        on workday_cost_center_site.cost_cntr_site_id = visits_actual.rpt_grp_3
    left join {{source('cdw', 'master_statistic')}} as master_statistic
        on master_statistic.stat_cd = visits_actual.mstr_op_spec_visit_cat_key
    left join {{source('cdw', 'location')}} as loc
        on loc.loc_key = visits_actual.loc_key
